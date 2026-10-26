mod support;

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hickory_proto::rr::Name;
use rand::SeedableRng;
use rand::rngs::StdRng;
use seedcrystal::cloudflare::api::{Batch, Client, OWNER_COMMENT};
use seedcrystal::cloudflare::publish::{
    CycleState, PublishConfig, reconcile, reconcile_once, spawn,
};
use seedcrystal::crawler::Snapshot;
use seedcrystal::store::Servable;
use support::fake_cloudflare::{BATCH_LIMIT, FakeCloudflare, FakeRecord};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const ZONE: &str = "zone1";
const TOKEN: &str = "s3cret-token-value";
const APEX: &str = "dnsseed.osmium.space";

async fn within<T>(f: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("test timed out")
}

async fn setup() -> (FakeCloudflare, Client) {
    let fake = FakeCloudflare::start(ZONE, TOKEN).await;
    let client = Client::new(&fake.base_url(), ZONE, TOKEN.to_string()).unwrap();
    (fake, client)
}

fn cfg(filters: Vec<u64>) -> PublishConfig {
    let mut c = PublishConfig::new(Name::from_ascii(format!("{APEX}.")).unwrap(), filters, 1);
    c.interval = Duration::from_millis(100);
    c
}

fn v4(i: u32, services: u64) -> Servable {
    let addr: SocketAddr = format!("203.{}.{}.1:23222", i % 250, i / 250)
        .parse()
        .unwrap();
    Servable { addr, services }
}

fn v6(i: u16, services: u64) -> Servable {
    let addr: SocketAddr = format!("[2001:{i:x}::1]:23222").parse().unwrap();
    Servable { addr, services }
}

fn snap(nodes: Vec<Servable>) -> Snapshot {
    Snapshot {
        nodes,
        built_at: 1000,
    }
}

fn rng() -> StdRng {
    StdRng::seed_from_u64(5)
}

/// (name, kind) -> addresses, owned records only.
fn owned_by_name(fake: &FakeCloudflare) -> HashMap<(String, String), Vec<String>> {
    let mut m: HashMap<(String, String), Vec<String>> = HashMap::new();
    for r in fake.records() {
        if r.comment.as_deref() == Some(OWNER_COMMENT) {
            m.entry((r.name, r.kind)).or_default().push(r.content);
        }
    }
    m
}

#[tokio::test]
async fn first_cycle_publishes_every_name() {
    let (fake, client) = setup().await;
    let mut nodes: Vec<_> = (0..25).map(|i| v4(i, 0x5)).collect();
    nodes.extend((0..3).map(|i| v6(i + 1, 0x5)));
    let c = cfg(vec![1, 5]);
    let report = within(reconcile_once(&client, &c, &snap(nodes), &mut rng()))
        .await
        .unwrap();
    assert_eq!(report.created, 3 * 23);
    assert_eq!((report.deleted, report.kept), (0, 0));
    assert!(report.foreign.is_empty() && !report.skipped_empty);
    let by = owned_by_name(&fake);
    for name in [APEX.to_string(), format!("x1.{APEX}"), format!("x5.{APEX}")] {
        assert_eq!(by[&(name.clone(), "A".into())].len(), 20, "{name}");
        assert_eq!(by[&(name, "AAAA".into())].len(), 3);
    }
    assert!(fake.records().iter().all(|r| r.ttl == 60 && !r.proxied));
    assert!(
        fake.records()
            .iter()
            .all(|r| r.comment.as_deref() == Some(OWNER_COMMENT))
    );
}

#[tokio::test]
async fn unchanged_snapshot_is_a_no_op() {
    let (fake, client) = setup().await;
    let s = snap((0..20).map(|i| v4(i, 0x5)).collect());
    let c = cfg(vec![1, 5]);
    within(reconcile_once(&client, &c, &s, &mut rng()))
        .await
        .unwrap();
    let before = fake.batch_ops().len();
    let report = within(reconcile_once(
        &client,
        &c,
        &s,
        &mut StdRng::seed_from_u64(99),
    ))
    .await
    .unwrap();
    assert_eq!(fake.batch_ops().len(), before);
    assert_eq!((report.created, report.deleted), (0, 0));
    assert_eq!(report.kept, 60);
}

#[tokio::test]
async fn minimal_churn() {
    let (fake, client) = setup().await;
    let nodes: Vec<_> = (0..25).map(|i| v4(i, 0x5)).collect();
    let c = cfg(vec![1]);
    within(reconcile_once(
        &client,
        &c,
        &snap(nodes.clone()),
        &mut rng(),
    ))
    .await
    .unwrap();
    // Drop one address that is currently published.
    let gone = fake
        .records()
        .iter()
        .find(|r| r.name == APEX)
        .unwrap()
        .content
        .clone();
    let rest: Vec<_> = nodes
        .into_iter()
        .filter(|n| n.addr.ip().to_string() != gone)
        .collect();
    let report = within(reconcile_once(&client, &c, &snap(rest), &mut rng()))
        .await
        .unwrap();
    assert_eq!((report.deleted, report.created), (2, 2));
    let last = fake.batch_ops().pop().unwrap();
    assert_eq!(last, (2, 2));
    let by = owned_by_name(&fake);
    assert_eq!(by[&(APEX.to_string(), "A".into())].len(), 20);
    assert!(!by[&(APEX.to_string(), "A".into())].contains(&gone));
}

#[tokio::test]
async fn empty_snapshot_never_deletes() {
    let (fake, client) = setup().await;
    fake.seed(FakeRecord::new(
        APEX,
        "A",
        "203.0.113.9",
        Some(OWNER_COMMENT),
    ));
    let report = within(reconcile_once(
        &client,
        &cfg(vec![1]),
        &snap(vec![]),
        &mut rng(),
    ))
    .await
    .unwrap();
    assert!(report.skipped_empty);
    assert!(fake.requests().is_empty());
    assert_eq!(fake.records().len(), 1);
}

#[tokio::test]
async fn foreign_records_untouched_and_reported() {
    let (fake, client) = setup().await;
    fake.seed(FakeRecord::new(APEX, "A", "198.51.100.1", None));
    fake.seed(FakeRecord::new(
        APEX,
        "A",
        "198.51.100.2",
        Some("something else"),
    ));
    let c = cfg(vec![1]);
    let s = snap((0..5).map(|i| v4(i, 0x1)).collect());
    for _ in 0..2 {
        let report = within(reconcile_once(&client, &c, &s, &mut rng()))
            .await
            .unwrap();
        assert_eq!(report.foreign, vec![APEX.to_string()]);
    }
    let recs = fake.records();
    for ip in ["198.51.100.1", "198.51.100.2"] {
        assert!(recs.iter().any(|r| r.content == ip), "{ip} lost");
    }
    // Even an empty servable set for the name never deletes foreign records.
    let only_other = snap(vec![v4(1, 0x0)]);
    within(reconcile_once(&client, &c, &only_other, &mut rng()))
        .await
        .unwrap();
    assert_eq!(
        fake.records()
            .iter()
            .filter(|r| r.comment.as_deref() != Some(OWNER_COMMENT))
            .count(),
        2
    );
}

#[tokio::test]
async fn over_200_ops_are_chunked() {
    let (fake, client) = setup().await;
    let nodes: Vec<_> = (0..70).map(|i| v4(i, 0xd)).collect();
    let mut c = cfg(vec![1, 5, 9, 0xd]);
    c.per_name = 50;
    c.seed_flags = 0xd;
    let report = within(reconcile_once(&client, &c, &snap(nodes), &mut rng()))
        .await
        .unwrap();
    assert_eq!(report.created, 250); // the apex plus four filter names, 50 each
    let ops = fake.batch_ops();
    assert!(ops.len() >= 2);
    assert!(ops.iter().all(|(d, p)| d + p <= BATCH_LIMIT && d + p > 0));
    assert_eq!(ops.iter().map(|(_, p)| p).sum::<usize>(), 250);
    assert_eq!(fake.records().len(), 250);
}

#[tokio::test]
async fn each_filter_name_holds_only_nodes_with_those_services() {
    let (fake, client) = setup().await;
    // Ten nodes with services 0x5 (203.0-9.0.1) and ten with 0x1 (203.10-19.0.1).
    let mut nodes: Vec<_> = (0..10).map(|i| v4(i, 0x5)).collect();
    nodes.extend((10..20).map(|i| v4(i, 0x1)));
    let c = cfg(vec![0x1, 0x5]);
    within(reconcile_once(&client, &c, &snap(nodes), &mut rng()))
        .await
        .unwrap();
    let by = owned_by_name(&fake);
    let sorted = |name: String| {
        let mut v = by[&(name, "A".to_string())].clone();
        v.sort();
        v
    };
    let mut with_0x5: Vec<String> = (0..10).map(|i| format!("203.{i}.0.1")).collect();
    with_0x5.sort();
    let mut everyone: Vec<String> = (0..20).map(|i| format!("203.{i}.0.1")).collect();
    everyone.sort();
    assert_eq!(sorted(format!("x5.{APEX}")), with_0x5);
    assert_eq!(sorted(format!("x1.{APEX}")), everyone);
    assert_eq!(sorted(APEX.to_string()), everyone);
}

#[tokio::test]
async fn one_name_over_the_batch_limit_is_split_and_fully_cleaned() {
    let (fake, client) = setup().await;
    // 250 owned records on the apex that no servable node backs.
    for i in 0..250 {
        fake.seed(FakeRecord::new(
            APEX,
            "A",
            &format!("198.18.{}.{}", i / 250, i % 250 + 1),
            Some(OWNER_COMMENT),
        ));
    }
    let s = snap((0..25).map(|i| v4(i, 0x5)).collect());
    let report = within(reconcile_once(&client, &cfg(vec![1]), &s, &mut rng()))
        .await
        .unwrap();
    assert_eq!(report.deleted, 250);
    let ops = fake.batch_ops();
    assert!(ops.len() >= 2, "{ops:?}");
    assert!(ops.iter().all(|(d, p)| d + p <= BATCH_LIMIT), "{ops:?}");
    assert_eq!(ops.iter().map(|(d, _)| d).sum::<usize>(), 250);
    assert!(
        fake.records()
            .iter()
            .all(|r| !r.content.starts_with("198.18.")),
        "a stale record survived"
    );
}

#[tokio::test]
async fn every_posted_record_says_proxied_false() {
    // The fake rejects a post without a boolean `proxied`, so a cycle that succeeds sent it.
    let (fake, client) = setup().await;
    let s = snap((0..5).map(|i| v4(i, 0x1)).collect());
    within(reconcile_once(&client, &cfg(vec![1]), &s, &mut rng()))
        .await
        .unwrap();
    assert_eq!(fake.records().len(), 10);
    assert!(fake.records().iter().all(|r| !r.proxied));
}

#[tokio::test]
async fn dry_run_never_applies() {
    let (fake, client) = setup().await;
    fake.seed(FakeRecord::new(
        APEX,
        "A",
        "203.0.113.9",
        Some(OWNER_COMMENT),
    ));
    let mut c = cfg(vec![1]);
    c.dry_run = true;
    let report = within(reconcile_once(
        &client,
        &c,
        &snap((0..5).map(|i| v4(i, 1)).collect()),
        &mut rng(),
    ))
    .await
    .unwrap();
    assert_eq!(report.deleted, 1);
    assert_eq!(report.created, 10);
    assert!(fake.batch_ops().is_empty());
    assert_eq!(fake.records().len(), 1);
}

#[tokio::test]
async fn duplicate_owned_records_collapse_to_one() {
    let (fake, client) = setup().await;
    for _ in 0..2 {
        fake.seed(FakeRecord::new(APEX, "A", "203.0.0.1", Some(OWNER_COMMENT)));
    }
    let report = within(reconcile_once(
        &client,
        &cfg(vec![]),
        &snap(vec![v4(0, 1)]),
        &mut rng(),
    ))
    .await
    .unwrap();
    assert_eq!((report.deleted, report.created, report.kept), (1, 0, 1));
    assert_eq!(fake.records().len(), 1);
}

#[tokio::test]
async fn ttl_below_60_is_rejected() {
    let (_fake, client) = setup().await;
    let mut c = cfg(vec![]);
    c.ttl = 30;
    assert!(c.validate().is_err());
    let (_tx, rx) = watch::channel(Arc::new(snap(vec![v4(0, 1)])));
    let handle = spawn(client, c, rx, CancellationToken::new());
    let err = within(handle).await.unwrap().unwrap_err();
    assert!(err.to_string().contains("TTL"), "{err}");
}

async fn batch_failure_waits_one_interval(status: u16) {
    let (fake, client) = setup().await;
    fake.fail_next_batch(status);
    let (_tx, rx) = watch::channel(Arc::new(snap((0..3).map(|i| v4(i, 1)).collect())));
    let stop = CancellationToken::new();
    let handle = spawn(client, cfg(vec![]), rx, stop.clone());
    within(async {
        while fake.records().len() < 3 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    stop.cancel();
    within(handle).await.unwrap().unwrap();
    let batches: Vec<_> = fake
        .requests()
        .into_iter()
        .filter(|r| r.ops.is_some())
        .collect();
    assert!(batches.len() >= 2, "no retry happened");
    let gap = batches[1].at.duration_since(batches[0].at);
    assert!(
        gap >= Duration::from_millis(100),
        "retried after only {gap:?}"
    );
}

#[tokio::test]
async fn server_error_backs_off_and_retries() {
    batch_failure_waits_one_interval(500).await;
}

#[tokio::test]
async fn rate_limit_backs_off_and_retries() {
    batch_failure_waits_one_interval(429).await;
}

#[tokio::test]
async fn foreign_record_holding_a_servable_address_does_not_stall() {
    let (fake, client) = setup().await;
    fake.seed(FakeRecord::new(APEX, "A", "203.0.0.1", None));
    fake.seed(FakeRecord::new(
        APEX,
        "AAAA",
        "2001:1::1",
        Some("hand made"),
    ));
    let mut nodes = vec![v4(0, 1), v4(1, 1)];
    nodes.push(v6(1, 1));
    let c = cfg(vec![]);
    let report = within(reconcile_once(&client, &c, &snap(nodes), &mut rng()))
        .await
        .unwrap();
    assert_eq!(report.created, 1);
    let recs = fake.records();
    assert_eq!(recs.len(), 3);
    assert_eq!(recs.iter().filter(|r| r.content == "203.0.0.1").count(), 1);
    assert!(
        recs.iter()
            .any(|r| r.content == "203.1.0.1" && r.comment.as_deref() == Some(OWNER_COMMENT))
    );
    // A second cycle is a no-op.
    let before = fake.batch_ops().len();
    let nodes = vec![v4(0, 1), v4(1, 1), v6(1, 1)];
    within(reconcile_once(&client, &c, &snap(nodes), &mut rng()))
        .await
        .unwrap();
    assert_eq!(fake.batch_ops().len(), before);
}

#[tokio::test]
async fn family_absent_from_snapshot_is_left_alone() {
    let (fake, client) = setup().await;
    fake.seed(FakeRecord::new(
        APEX,
        "AAAA",
        "2001:db8::7",
        Some(OWNER_COMMENT),
    ));
    fake.seed(FakeRecord::new(APEX, "A", "203.0.9.9", Some(OWNER_COMMENT)));
    let report = within(reconcile_once(
        &client,
        &cfg(vec![1]),
        &snap(vec![v4(0, 1), v4(1, 1)]),
        &mut rng(),
    ))
    .await
    .unwrap();
    assert_eq!(report.deleted, 1);
    let recs = fake.records();
    assert!(
        recs.iter()
            .any(|r| r.kind == "AAAA" && r.content == "2001:db8::7")
    );
    assert!(!recs.iter().any(|r| r.content == "203.0.9.9"));
    assert_eq!(recs.iter().filter(|r| r.kind == "A").count(), 4);
}

#[tokio::test]
async fn per_name_outside_1_to_50_is_rejected() {
    let mut c = cfg(vec![]);
    for bad in [0, 51] {
        c.per_name = bad;
        assert!(c.validate().is_err());
    }
    for good in [1, 50] {
        c.per_name = good;
        assert!(c.validate().is_ok());
    }
}

#[tokio::test]
async fn waits_for_a_non_empty_snapshot() {
    let (fake, client) = setup().await;
    let (tx, rx) = watch::channel(Arc::new(snap(vec![])));
    let stop = CancellationToken::new();
    let handle = spawn(client, cfg(vec![]), rx, stop.clone());
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Only the token check (apex lookups) has happened; nothing was written.
    assert!(fake.batch_ops().is_empty());
    assert!(fake.requests().iter().all(|r| r.method == "GET"));
    tx.send(Arc::new(snap(vec![v4(0, 1)]))).unwrap();
    within(async {
        while fake.records().is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    stop.cancel();
    within(handle).await.unwrap().unwrap();
}

#[tokio::test]
async fn auth_error_stops_publisher() {
    let (fake, client) = setup().await;
    fake.fail_always(Some(401));
    let (_tx, rx) = watch::channel(Arc::new(snap(vec![v4(0, 1)])));
    let handle = spawn(client, cfg(vec![]), rx, CancellationToken::new());
    let err = within(handle).await.unwrap().unwrap_err();
    let text = format!("{err:#}");
    assert!(text.contains("DNS"), "{text}");
    assert!(!text.contains(TOKEN));
}

fn nodes(n: u32) -> Vec<Servable> {
    (0..n).map(|i| v4(i, 1)).collect()
}

async fn cycle(
    client: &Client,
    c: &PublishConfig,
    n: u32,
    state: &mut CycleState,
) -> seedcrystal::cloudflare::publish::CycleReport {
    within(reconcile(
        client,
        c,
        &snap(nodes(n)),
        &mut rng(),
        state,
        &CancellationToken::new(),
    ))
    .await
    .unwrap()
}

#[tokio::test]
async fn shrink_guard_keeps_records_then_recovers() {
    let (fake, client) = setup().await;
    let c = cfg(vec![]);
    let mut state = CycleState::default();
    let r = cycle(&client, &c, 25, &mut state).await;
    assert!(!r.shrink_guard);
    assert_eq!(fake.records().len(), 20);

    let before = fake.records();
    let r = cycle(&client, &c, 2, &mut state).await;
    assert!(r.shrink_guard);
    assert_eq!(r.deleted, 0);
    assert_eq!(fake.records(), before, "records must be intact");

    // Recovery: normal reconciliation resumes. Records outside the snapshot's 25 go.
    let r = cycle(&client, &c, 25, &mut state).await;
    assert!(!r.shrink_guard);
    assert_eq!(fake.records().len(), 20);
}

#[tokio::test]
async fn shrink_guard_still_fills_gaps() {
    let (fake, client) = setup().await;
    let mut c = cfg(vec![]);
    c.per_name = 10;
    let mut state = CycleState::default();
    cycle(&client, &c, 25, &mut state).await;
    // Cloudflare lost five records out of band; the snapshot shrank to 4 servable nodes.
    let lost: Vec<String> = fake.records().into_iter().take(5).map(|r| r.id).collect();
    within(client.apply(&Batch {
        deletes: lost,
        posts: vec![],
    }))
    .await
    .unwrap();
    let r = cycle(&client, &c, 4, &mut state).await;
    assert!(r.shrink_guard);
    assert_eq!(r.deleted, 0);
    // The gap is filled from the 4 servable nodes (those not already published).
    assert!(fake.records().len() >= 5);
}

#[tokio::test]
async fn gradual_decline_deletes_only_while_above_half_the_mark() {
    let (fake, client) = setup().await;
    let mut c = cfg(vec![]);
    c.per_name = 25;
    let mut state = CycleState::default();
    cycle(&client, &c, 25, &mut state).await;
    for (n, guarded) in [(20, false), (15, false), (10, true)] {
        let r = cycle(&client, &c, n, &mut state).await;
        assert_eq!(r.shrink_guard, guarded, "{n}");
        if guarded {
            assert_eq!(r.deleted, 0);
            assert_eq!(fake.records().len(), 15, "stale records stay");
        } else {
            assert_eq!(fake.records().len(), n as usize);
        }
    }
}

#[tokio::test]
async fn rotation_swaps_two_per_name_and_family_each_cycle() {
    let (fake, client) = setup().await;
    let c = cfg(vec![1]);
    let mut all = nodes(40);
    all.extend((0..40).map(|i| v6(i + 1, 1)));
    let s = snap(all);
    within(reconcile_once(&client, &c, &s, &mut rng()))
        .await
        .unwrap();
    let mut state = CycleState::default();
    let mut seen: std::collections::HashSet<String> =
        fake.records().into_iter().map(|r| r.content).collect();
    for round in 0..4 {
        let before: std::collections::HashSet<_> =
            fake.records().into_iter().map(|r| r.id).collect();
        let r = within(reconcile(
            &client,
            &c,
            &s,
            &mut StdRng::seed_from_u64(round),
            &mut state,
            &CancellationToken::new(),
        ))
        .await
        .unwrap();
        // 2 names x 2 families x 2 swaps.
        assert_eq!((r.deleted, r.created), (8, 8), "round {round}");
        let after = fake.records();
        assert_eq!(after.len(), 80);
        assert_eq!(after.iter().filter(|x| before.contains(&x.id)).count(), 72);
        seen.extend(after.into_iter().map(|r| r.content));
    }
    assert!(seen.len() > 40, "rotation introduced new addresses");
}

#[tokio::test]
async fn no_rotation_when_everything_fits() {
    let (fake, client) = setup().await;
    let c = cfg(vec![]);
    let s = snap(nodes(20));
    within(reconcile_once(&client, &c, &s, &mut rng()))
        .await
        .unwrap();
    let before = fake.batch_ops().len();
    let r = within(reconcile_once(&client, &c, &s, &mut rng()))
        .await
        .unwrap();
    assert_eq!((r.created, r.deleted), (0, 0));
    assert_eq!(fake.batch_ops().len(), before);
}

#[tokio::test]
async fn no_rotation_in_a_skipped_family() {
    let (fake, client) = setup().await;
    let c = cfg(vec![]);
    // 30 IPv6 records published earlier; the snapshot has only IPv4 now.
    let mut c6 = c.clone();
    c6.per_name = 20;
    let both: Vec<_> = nodes(3)
        .into_iter()
        .chain((0..30).map(|i| v6(i + 1, 1)))
        .collect();
    within(reconcile_once(&client, &c6, &snap(both), &mut rng()))
        .await
        .unwrap();
    let v6_before: Vec<_> = fake
        .records()
        .into_iter()
        .filter(|r| r.kind == "AAAA")
        .collect();
    let s = snap(nodes(3));
    within(reconcile_once(&client, &c6, &s, &mut rng()))
        .await
        .unwrap();
    let v6_after: Vec<_> = fake
        .records()
        .into_iter()
        .filter(|r| r.kind == "AAAA")
        .collect();
    assert_eq!(v6_before, v6_after);
}

#[tokio::test]
async fn wrong_ttl_and_proxied_records_are_replaced() {
    let (fake, client) = setup().await;
    let mut a = FakeRecord::new(APEX, "A", "203.0.0.1", Some(OWNER_COMMENT));
    a.ttl = 300;
    let mut b = FakeRecord::new(APEX, "A", "203.1.0.1", Some(OWNER_COMMENT));
    b.ttl = 60;
    b.proxied = true;
    let mut ok = FakeRecord::new(APEX, "A", "203.2.0.1", Some(OWNER_COMMENT));
    ok.ttl = 60;
    for r in [a, b, ok] {
        fake.seed(r);
    }
    let r = within(reconcile_once(
        &client,
        &cfg(vec![]),
        &snap(nodes(3)),
        &mut rng(),
    ))
    .await
    .unwrap();
    assert_eq!((r.deleted, r.created, r.kept), (2, 2, 1));
    let recs = fake.records();
    assert_eq!(recs.len(), 3);
    assert!(recs.iter().all(|r| r.ttl == 60 && !r.proxied));
}

#[tokio::test]
async fn wrong_token_with_empty_snapshot_stops_at_once() {
    let (_fake, _client) = setup().await;
    let (fake, _) = setup().await;
    let client = Client::new(&fake.base_url(), ZONE, "wrong".to_string()).unwrap();
    let (_tx, rx) = watch::channel(Arc::new(snap(vec![])));
    let handle = spawn(client, cfg(vec![]), rx, CancellationToken::new());
    let err = within(handle).await.unwrap().unwrap_err();
    assert!(format!("{err:#}").contains("DNS"), "{err:#}");
}

#[tokio::test]
async fn a_non_auth_preflight_error_does_not_stop_the_loop() {
    let (fake, client) = setup().await;
    fake.fail_next(500);
    let (_tx, rx) = watch::channel(Arc::new(snap(nodes(2))));
    let stop = CancellationToken::new();
    let handle = spawn(client, cfg(vec![]), rx, stop.clone());
    within(async {
        while fake.records().len() < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    stop.cancel();
    within(handle).await.unwrap().unwrap();
}

#[tokio::test]
async fn shutdown_stops_a_cycle_between_api_calls() {
    let (fake, client) = setup().await;
    fake.set_delay(Duration::from_millis(150));
    let filters: Vec<u64> = (1..=8).collect();
    let c = cfg(filters);
    let stop = CancellationToken::new();
    let s = snap(nodes(3));
    let canceller = {
        let stop = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            stop.cancel();
        })
    };
    let r = within(reconcile(
        &client,
        &c,
        &s,
        &mut rng(),
        &mut CycleState::default(),
        &stop,
    ))
    .await
    .unwrap();
    canceller.await.unwrap();
    let lists = fake.requests().iter().filter(|r| r.method == "GET").count();
    // 9 names x 2 kinds would be 18 list calls; shutdown cut it short.
    assert!(lists < 18, "{lists}");
    assert!(fake.batch_ops().is_empty());
    assert_eq!(r.deleted, 0);
}

#[tokio::test]
async fn a_wrong_zone_stops_the_publisher_quickly() {
    let fake = FakeCloudflare::start(ZONE, TOKEN).await;
    let client = Client::new(&fake.base_url(), "wrong", TOKEN.to_string()).unwrap();
    let (_tx, rx) = watch::channel(Arc::new(snap(vec![v4(0, 1)])));
    let handle = spawn(client, cfg(vec![]), rx, CancellationToken::new());
    let err = within(handle).await.unwrap().unwrap_err();
    let text = format!("{err:#}");
    assert!(
        text.contains("zone_id was not found or is malformed"),
        "{text}"
    );
    assert!(!text.contains(TOKEN));
}

fn with_state(path: &std::path::Path) -> PublishConfig {
    let mut c = cfg(vec![]);
    c.state_path = Some(path.to_path_buf());
    c
}

/// Runs a publisher until it has made a request round after `ready`, then stops it.
async fn run_publisher(fake: &FakeCloudflare, c: PublishConfig, n: u32, cycles: usize) {
    let client = Client::new(&fake.base_url(), ZONE, TOKEN.to_string()).unwrap();
    let (_tx, rx) = watch::channel(Arc::new(snap(nodes(n))));
    let stop = CancellationToken::new();
    let before = fake.requests().len();
    let handle = spawn(client, c, rx, stop.clone());
    within(async {
        // The startup check is 2 GETs, each cycle 2 more plus any batch.
        while fake.requests().len() < before + 2 + 2 * cycles {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    stop.cancel();
    within(handle).await.unwrap().unwrap();
}

#[tokio::test]
async fn the_shrink_guard_survives_a_restart() {
    let (fake, _client) = setup().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("guard.json");
    run_publisher(&fake, with_state(&path), 25, 1).await;
    assert_eq!(fake.records().len(), 20);
    assert!(path.exists());

    let before = fake.records();
    let batches = fake.batch_ops().len();
    run_publisher(&fake, with_state(&path), 2, 1).await;
    assert_eq!(fake.records().len(), before.len());
    // Only creates, if any: nothing was deleted.
    assert!(fake.batch_ops()[batches..].iter().all(|&(d, _)| d == 0));
    for r in &before {
        assert!(fake.records().iter().any(|x| x.content == r.content));
    }
}

#[tokio::test]
async fn samples_older_than_24_hours_in_the_file_are_ignored() {
    let (fake, _client) = setup().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("guard.json");
    let old = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 25 * 3600;
    std::fs::write(&path, format!(r#"{{"samples": [[{old}, 1000]]}}"#)).unwrap();
    // A stale owned record is deleted only when the guard is off.
    fake.seed(FakeRecord::new(
        APEX,
        "A",
        "198.51.100.9",
        Some(OWNER_COMMENT),
    ));
    run_publisher(&fake, with_state(&path), 25, 1).await;
    assert!(fake.records().iter().all(|r| r.content != "198.51.100.9"));
    assert_eq!(fake.records().len(), 20);
}

#[tokio::test]
async fn a_corrupt_state_file_starts_empty_and_the_publisher_runs() {
    let (fake, _client) = setup().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("guard.json");
    std::fs::write(&path, "{not json").unwrap();
    run_publisher(&fake, with_state(&path), 25, 1).await;
    assert_eq!(fake.records().len(), 20);
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(
        serde_json::from_str::<serde_json::Value>(&text).is_ok(),
        "{text}"
    );
}

#[tokio::test]
async fn an_unwritable_state_path_does_not_stop_the_publisher() {
    let (fake, _client) = setup().await;
    let dir = tempfile::tempdir().unwrap();
    let c = with_state(&dir.path().join("missing-dir").join("guard.json"));
    run_publisher(&fake, c, 25, 1).await;
    assert_eq!(fake.records().len(), 20);
}

#[tokio::test]
async fn a_zone_fault_after_success_is_retried_not_fatal() {
    let (fake, client) = setup().await;
    let (_tx, rx) = watch::channel(Arc::new(snap(nodes(5))));
    let stop = CancellationToken::new();
    let mut c = cfg(vec![]);
    c.interval = Duration::from_millis(50);
    let handle = spawn(client, c, rx, stop.clone());
    within(async {
        while fake.records().len() < 5 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    fake.zone_fault(true);
    let seen = fake.requests().len();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        !handle.is_finished(),
        "the publisher must survive the fault"
    );
    assert!(fake.requests().len() > seen, "it should keep trying");
    fake.zone_fault(false);
    // Recovery: a record removed behind its back is put back once calls work again.
    let victim = fake.records()[0].clone();
    fake.remove_content(&victim.content);
    within(async {
        while fake.records().len() < 5 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    stop.cancel();
    within(handle).await.unwrap().unwrap();
}

/// Host `h` of the /16 `198.51.0.0`.
fn big(h: u32) -> Servable {
    Servable {
        addr: format!("198.51.0.{h}:23222").parse().unwrap(),
        services: 1,
    }
}

fn in_big_group(content: &str) -> bool {
    content.starts_with("198.51.")
}

fn a_contents(fake: &FakeCloudflare, name: &str) -> Vec<String> {
    fake.records()
        .into_iter()
        .filter(|r| r.name == name && r.kind == "A")
        .map(|r| r.content)
        .collect()
}

fn max_per_slash16(contents: &[String]) -> usize {
    let mut m: HashMap<String, usize> = HashMap::new();
    for c in contents {
        let g = c.split('.').take(2).collect::<Vec<_>>().join(".");
        *m.entry(g).or_default() += 1;
    }
    m.into_values().max().unwrap_or(0)
}

#[tokio::test]
async fn a_fresh_zone_takes_two_per_group_and_fills_from_the_rest() {
    let (fake, client) = setup().await;
    let mut all: Vec<_> = (1..=25).map(big).collect();
    all.extend((0..25).map(|i| v4(i, 1)));
    let c = cfg(vec![1]);
    let report = within(reconcile_once(&client, &c, &snap(all), &mut rng()))
        .await
        .unwrap();
    assert_eq!(report.created, 40);
    for name in [APEX.to_string(), format!("x1.{APEX}")] {
        let got = a_contents(&fake, &name);
        assert_eq!(got.len(), 20, "{name}");
        assert_eq!(got.iter().filter(|c| in_big_group(c)).count(), 2, "{name}");
        assert_eq!(max_per_slash16(&got), 2);
    }
}

#[tokio::test]
async fn fewer_groups_than_slots_publishes_fewer() {
    let (fake, client) = setup().await;
    let mut all: Vec<_> = (1..=25).map(big).collect();
    all.extend((0..3).map(|i| v4(i, 1)));
    within(reconcile_once(
        &client,
        &cfg(vec![]),
        &snap(all),
        &mut rng(),
    ))
    .await
    .unwrap();
    assert_eq!(a_contents(&fake, APEX).len(), 5);
}

fn seed_owned(fake: &FakeCloudflare, ips: &[String]) {
    for ip in ips {
        fake.seed(FakeRecord::new(APEX, "A", ip, Some(OWNER_COMMENT)));
    }
}

#[tokio::test]
async fn owned_records_over_the_group_limit_are_deleted_and_refilled() {
    let (fake, client) = setup().await;
    let held: Vec<String> = (1..=6).map(|h| format!("198.51.0.{h}")).collect();
    seed_owned(&fake, &held);
    let mut all: Vec<_> = (1..=6).map(big).collect();
    all.extend((0..25).map(|i| v4(i, 1)));
    let report = within(reconcile_once(
        &client,
        &cfg(vec![]),
        &snap(all),
        &mut rng(),
    ))
    .await
    .unwrap();
    assert_eq!(report.deleted, 4);
    assert_eq!(report.kept, 2);
    assert_eq!(report.created, 18);
    let got = a_contents(&fake, APEX);
    assert_eq!(got.len(), 20);
    // The first two in listing order stay.
    let mut big_left: Vec<_> = got.iter().filter(|c| in_big_group(c)).cloned().collect();
    big_left.sort();
    assert_eq!(big_left, ["198.51.0.1", "198.51.0.2"]);
}

#[tokio::test]
async fn the_shrink_guard_deletes_no_excess_records() {
    let (fake, client) = setup().await;
    let mut state = CycleState::default();
    let c = cfg(vec![]);
    // Set a high-water mark of 60.
    within(reconcile(
        &client,
        &c,
        &snap(nodes(60)),
        &mut rng(),
        &mut state,
        &CancellationToken::new(),
    ))
    .await
    .unwrap();
    for r in fake.records() {
        fake.remove_content(&r.content);
    }
    let held: Vec<String> = (1..=6).map(|h| format!("198.51.0.{h}")).collect();
    seed_owned(&fake, &held);
    let mut small: Vec<_> = (1..=6).map(big).collect();
    small.extend((0..14).map(|i| v4(i, 1)));
    let r = within(reconcile(
        &client,
        &c,
        &snap(small),
        &mut rng(),
        &mut state,
        &CancellationToken::new(),
    ))
    .await
    .unwrap();
    assert!(r.shrink_guard);
    assert_eq!(r.deleted, 0);
    let got = a_contents(&fake, APEX);
    assert_eq!(got.iter().filter(|c| in_big_group(c)).count(), 6);
    // The fill still respects the limit: the six held fill their group, so new ones come
    // from the other groups only.
    assert_eq!(got.len(), 20);
}

#[tokio::test]
async fn rotation_never_exceeds_the_group_limit() {
    let (fake, client) = setup().await;
    let mut c = cfg(vec![]);
    c.per_name = 6;
    // 10 groups of 4 nodes each.
    let all: Vec<Servable> = (0..10u32)
        .flat_map(|g| {
            (1..=4u32).map(move |h| Servable {
                addr: format!("203.{g}.0.{h}:23222").parse().unwrap(),
                services: 1,
            })
        })
        .collect();
    let s = snap(all);
    within(reconcile_once(&client, &c, &s, &mut rng()))
        .await
        .unwrap();
    let mut state = CycleState::default();
    let mut seen = std::collections::HashSet::new();
    for round in 0..30 {
        within(reconcile(
            &client,
            &c,
            &s,
            &mut StdRng::seed_from_u64(round),
            &mut state,
            &CancellationToken::new(),
        ))
        .await
        .unwrap();
        let got = a_contents(&fake, APEX);
        assert_eq!(got.len(), 6, "round {round}");
        assert!(max_per_slash16(&got) <= 2, "round {round}: {got:?}");
        seen.extend(got);
    }
    assert!(seen.len() > 6, "rotation introduced new addresses");
}

fn seed_kind(fake: &FakeCloudflare, kind: &str, ips: &[&str], comment: Option<&str>) {
    for ip in ips {
        fake.seed(FakeRecord::new(APEX, kind, ip, comment));
    }
}

fn v6_in(group: u16, host: u16) -> Servable {
    Servable {
        addr: format!("[2001:{group:x}::{host:x}]:23222").parse().unwrap(),
        services: 1,
    }
}

fn sorted_contents(fake: &FakeCloudflare, kind: &str) -> Vec<String> {
    let mut v: Vec<String> = fake
        .records()
        .into_iter()
        .filter(|r| r.kind == kind)
        .map(|r| r.content)
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn survivors_do_not_depend_on_listing_order() {
    let mut results = Vec::new();
    for order in [[1, 2, 3, 4, 5, 6], [6, 5, 4, 3, 2, 1]] {
        let (fake, client) = setup().await;
        let ips: Vec<String> = order.iter().map(|h| format!("198.51.0.{h}")).collect();
        seed_owned(&fake, &ips);
        let mut all: Vec<_> = (1..=6).map(big).collect();
        all.extend((0..25).map(|i| v4(i, 1)));
        let report = within(reconcile_once(
            &client,
            &cfg(vec![]),
            &snap(all),
            &mut rng(),
        ))
        .await
        .unwrap();
        let big_left: Vec<String> = sorted_contents(&fake, "A")
            .into_iter()
            .filter(|c| in_big_group(c))
            .collect();
        results.push((report.deleted, report.kept, report.created, big_left));
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0].3, ["198.51.0.1", "198.51.0.2"]);
}

#[tokio::test]
async fn a_foreign_address_in_a_full_group_neither_counts_nor_is_posted() {
    let (fake, client) = setup().await;
    seed_kind(&fake, "A", &["198.51.0.9"], None);
    seed_owned(&fake, &["198.51.0.1".into(), "198.51.0.2".into()]);
    let mut all: Vec<_> = (1..=9).map(big).collect();
    all.extend((0..25).map(|i| v4(i, 1)));
    let report = within(reconcile_once(
        &client,
        &cfg(vec![]),
        &snap(all),
        &mut rng(),
    ))
    .await
    .unwrap();
    assert_eq!(report.deleted, 0);
    let recs = fake.records();
    // The foreign record is untouched and not duplicated; the group's two owned records are
    // the only owned ones there, so the foreign one did not use up the limit.
    assert_eq!(recs.iter().filter(|r| r.content == "198.51.0.9").count(), 1);
    let owned_big = recs
        .iter()
        .filter(|r| in_big_group(&r.content) && r.comment.as_deref() == Some(OWNER_COMMENT))
        .count();
    assert_eq!(owned_big, 2);
    assert_eq!(a_contents(&fake, APEX).len(), 21);
}

#[tokio::test]
async fn a_foreign_address_does_not_use_up_its_groups_limit() {
    let (fake, client) = setup().await;
    seed_kind(&fake, "A", &["198.51.0.9"], None);
    let all: Vec<_> = (1..=9).map(big).collect();
    within(reconcile_once(
        &client,
        &cfg(vec![]),
        &snap(all),
        &mut rng(),
    ))
    .await
    .unwrap();
    let owned_big: Vec<_> = fake
        .records()
        .into_iter()
        .filter(|r| r.comment.as_deref() == Some(OWNER_COMMENT))
        .collect();
    assert_eq!(owned_big.len(), 2);
    assert!(owned_big.iter().all(|r| r.content != "198.51.0.9"));
}

#[tokio::test]
async fn ipv6_groups_are_limited_through_reconcile() {
    let (fake, client) = setup().await;
    let mut all: Vec<_> = (1..=10).map(|h| v6_in(0xabc, h)).collect();
    all.extend((1..=30).map(|g| v6_in(g, 1)));
    let report = within(reconcile_once(
        &client,
        &cfg(vec![]),
        &snap(all),
        &mut rng(),
    ))
    .await
    .unwrap();
    assert_eq!(report.created, 20);
    let got = sorted_contents(&fake, "AAAA");
    assert_eq!(got.len(), 20);
    assert_eq!(got.iter().filter(|c| c.starts_with("2001:abc:")).count(), 2);
}

#[tokio::test]
async fn a_skipped_family_is_untouched_even_over_the_group_limit() {
    let (fake, client) = setup().await;
    let held = ["2001:abc::1", "2001:abc::2", "2001:abc::3", "2001:abc::4"];
    seed_kind(&fake, "AAAA", &held, Some(OWNER_COMMENT));
    let before = sorted_contents(&fake, "AAAA");
    within(reconcile_once(
        &client,
        &cfg(vec![]),
        &snap(nodes(3)),
        &mut rng(),
    ))
    .await
    .unwrap();
    assert_eq!(sorted_contents(&fake, "AAAA"), before);
}

#[tokio::test]
async fn under_the_shrink_guard_a_retained_excess_blocks_the_fill_in_that_group() {
    let (fake, client) = setup().await;
    let mut state = CycleState::default();
    let c = cfg(vec![]);
    within(reconcile(
        &client,
        &c,
        &snap(nodes(60)),
        &mut rng(),
        &mut state,
        &CancellationToken::new(),
    ))
    .await
    .unwrap();
    for r in fake.records() {
        fake.remove_content(&r.content);
    }
    let held: Vec<String> = (1..=3).map(|h| format!("198.51.0.{h}")).collect();
    seed_owned(&fake, &held);
    // Plenty of unpublished nodes in the over-full group, and a few elsewhere.
    let mut small: Vec<_> = (1..=12).map(big).collect();
    small.extend((0..8).map(|i| v4(i, 1)));
    let r = within(reconcile(
        &client,
        &c,
        &snap(small),
        &mut rng(),
        &mut state,
        &CancellationToken::new(),
    ))
    .await
    .unwrap();
    assert!(r.shrink_guard && r.deleted == 0);
    let got = a_contents(&fake, APEX);
    assert_eq!(got.iter().filter(|c| in_big_group(c)).count(), 3);
    assert_eq!(got.len(), 11);
}

#[tokio::test]
async fn an_underfilled_name_does_not_swap_within_a_group() {
    let (fake, client) = setup().await;
    let c = cfg(vec![]);
    // Two groups of four: the limit leaves the name at 4 of 20 slots, with fresh addresses
    // in the same groups as the published ones.
    let all: Vec<Servable> = (0..2u32)
        .flat_map(|g| {
            (1..=4u32).map(move |h| Servable {
                addr: format!("203.{g}.0.{h}:23222").parse().unwrap(),
                services: 1,
            })
        })
        .collect();
    let s = snap(all);
    within(reconcile_once(&client, &c, &s, &mut rng()))
        .await
        .unwrap();
    let before = fake.records();
    assert_eq!(before.len(), 4);
    let ops = fake.batch_ops().len();
    let mut state = CycleState::default();
    for round in 0..10 {
        let r = within(reconcile(
            &client,
            &c,
            &s,
            &mut StdRng::seed_from_u64(round),
            &mut state,
            &CancellationToken::new(),
        ))
        .await
        .unwrap();
        assert_eq!((r.created, r.deleted), (0, 0), "round {round}");
    }
    assert_eq!(fake.batch_ops().len(), ops);
    assert_eq!(fake.records(), before);
}
