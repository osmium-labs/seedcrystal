mod support;

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use hickory_proto::op::ResponseCode;
use hickory_proto::rr::rdata::{A, NS};
use hickory_proto::rr::{Name, RData, RecordType};
use seedcrystal::cloudflare::api::OWNER_COMMENT;
use seedcrystal::coin::CoinPreset;
use seedcrystal::config::{self, NetworkConfig};
use seedcrystal::dns::querylog::{self, NameStats, QueryLog};
use seedcrystal::doctor::{Check, DoctorOptions, Status, run_checks};
use support::fake_cloudflare::{FakeCloudflare, FakeRecord};
use support::fake_resolver::{FakeResolver, Reply};
use support::{FakePeer, FakeScript};

const SEED: &str = "dnsseed.osmium.space";
const NS_NAME: &str = "seed-ns.osmium.space";

struct Fixture {
    dir: tempfile::TempDir,
    cfg_path: PathBuf,
    net: NetworkConfig,
    preset: CoinPreset,
}

impl Fixture {
    /// An authoritative network whose coin listens on `port`.
    fn authoritative(port: u16) -> Self {
        Self::build(port, "")
    }

    fn cloudflare(port: u16, api_base: &str, zone: &str) -> Self {
        Self::build(
            port,
            &format!(
                "publish = \"cloudflare\"\n[network.cloudflare]\nzone_id = \"{zone}\"\n\
                 token_file = \"@DIR@/token\"\napi_base = \"{api_base}\"\n"
            ),
        )
    }

    fn build(port: u16, tail: &str) -> Self {
        // An exported token would clash with the fixture's token_file.
        static CLEAR_TOKEN: std::sync::Once = std::sync::Once::new();
        CLEAR_TOKEN.call_once(|| {
            // SAFETY: runs once before any fixture exists, and only removes a variable.
            unsafe { std::env::remove_var("SEEDCRYSTAL_CLOUDFLARE_TOKEN") };
        });
        let dir = tempfile::tempdir().unwrap();
        let mut preset = include_str!("../presets/osmium-mainnet.toml").to_string();
        for key in ["bootstrap_dns", "bootstrap_fixed"] {
            let start = preset.find(&format!("{key} =")).unwrap();
            let end = start + preset[start..].find('\n').unwrap();
            preset.replace_range(start..end, &format!("{key} = []"));
        }
        let preset = preset.replace("default_port = 9969", &format!("default_port = {port}"));
        std::fs::write(dir.path().join("fake.toml"), preset).unwrap();
        std::fs::write(dir.path().join("token"), "secret-token\n").unwrap();
        let auth = if tail.is_empty() {
            format!("nameserver = \"{NS_NAME}\"\nmailbox = \"hostmaster.osmium.space\"\n")
        } else {
            String::new()
        };
        let cfg_path = dir.path().join("seedcrystal.toml");
        // Keys before the first sub-table, so `tail` may open `[network.cloudflare]`.
        let tail = tail.replace("@DIR@", &dir.path().display().to_string());
        let (keys, table) = match tail.find("[network") {
            Some(i) => tail.split_at(i),
            None => (tail.as_str(), ""),
        };
        std::fs::write(
            &cfg_path,
            format!(
                "[[network]]\npreset_file = \"fake.toml\"\nseed = \"{SEED}\"\n{auth}\
                 data_dir = \"{}\"\n{keys}{table}",
                dir.path().join("data").display()
            ),
        )
        .unwrap();
        let cfg = config::load(&cfg_path).unwrap();
        let net = cfg.network.into_iter().next().unwrap();
        let preset = net.load_preset().unwrap();
        Self {
            dir,
            cfg_path,
            net,
            preset,
        }
    }

    fn log_path(&self) -> PathBuf {
        querylog::path_for(&self.net.data_dir, &self.preset.id)
    }

    fn write_log(&self, name: &str, answered: u64, last_seen: u64) {
        std::fs::create_dir_all(&self.net.data_dir).unwrap();
        let mut log = QueryLog {
            since: last_seen.saturating_sub(100),
            ..QueryLog::default()
        };
        log.names.insert(
            name.to_string(),
            NameStats {
                answered,
                empty: 0,
                last_seen,
            },
        );
        std::fs::write(self.log_path(), serde_json::to_string(&log).unwrap()).unwrap();
    }

    fn options(&self, resolvers: &[&FakeResolver]) -> DoctorOptions {
        DoctorOptions {
            resolvers: resolvers.iter().map(|r| r.addr).collect(),
            timeout: Duration::from_secs(15),
            probe_max: 5,
        }
    }
}

fn a(ip: &str) -> RData {
    RData::A(A(ip.parse::<Ipv4Addr>().unwrap()))
}

fn ns(name: &str) -> RData {
    RData::NS(NS(Name::from_ascii(name).unwrap()))
}

/// A resolver that serves the plain name and `x1.` and delegates the seed correctly.
async fn healthy_resolver() -> FakeResolver {
    let r = FakeResolver::start().await;
    r.answers(SEED, RecordType::NS, vec![ns("seed-ns.osmium.space.")])
        .answers(NS_NAME, RecordType::A, vec![a("192.0.2.53")])
        .answers(SEED, RecordType::A, vec![a("127.0.0.1")])
        .answers(&format!("x1.{SEED}"), RecordType::A, vec![a("127.0.0.1")])
        .answers(SEED, RecordType::AAAA, vec![])
        .answers(&format!("x1.{SEED}"), RecordType::AAAA, vec![]);
    r
}

fn now() -> u64 {
    querylog::unix_now()
}

fn step(checks: &[Check], n: u8) -> &Check {
    checks
        .iter()
        .find(|c| c.step == n)
        .unwrap_or_else(|| panic!("no step {n} in {checks:#?}"))
}

fn peer_for(preset_version: i32) -> (SocketAddr, tokio::task::JoinHandle<Vec<String>>) {
    let magic = seedcrystal::coin::builtin("osmium-mainnet").unwrap().magic;
    FakePeer::spawn(FakeScript::new(magic, preset_version))
}

/// A port where something accepts connections and closes them at once: it answers, but it is
/// not a node. The listener stays bound, so no concurrent test can be handed the port.
fn not_a_node() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            drop(conn);
        }
    });
    port
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test(flavor = "multi_thread")]
async fn healthy_authoritative_seed_passes_steps_1_to_4() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    fx.write_log(&format!("x1.{SEED}"), 3, now() - 60);
    let r = healthy_resolver().await;
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    for n in 1..=4 {
        assert_eq!(step(&checks, n).status, Status::Pass, "{checks:#?}");
    }
    assert!(step(&checks, 3).detail.contains("127.0.0.1"));
    assert!(step(&checks, 3).detail.contains("70231"));
    assert!(step(&checks, 4).detail.contains("dig"), "honest wording");
    let seen = r.seen();
    assert!(seen.iter().all(|s| s.rd), "queries are sent with RD set");
    assert!(
        seen.iter()
            .any(|s| s.name == format!("x1.{SEED}") && s.kind == RecordType::A)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn x1_nxdomain_fails_with_the_node_name() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    fx.write_log(SEED, 3, now());
    let r = FakeResolver::start().await;
    r.answers(SEED, RecordType::NS, vec![ns("seed-ns.osmium.space.")])
        .answers(NS_NAME, RecordType::A, vec![a("192.0.2.53")])
        .answers(SEED, RecordType::A, vec![a("127.0.0.1")])
        .answers(SEED, RecordType::AAAA, vec![]);
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s2 = step(&checks, 2);
    assert_eq!(s2.status, Status::Fail, "{checks:#?}");
    let fix = s2.fix.as_deref().expect("a fix line");
    assert!(fix.contains("x1."), "{fix}");
    assert!(fix.contains("NXDOMAIN"), "{fix}");
    assert!(fix.contains(&format!("x1.{SEED}")), "{fix}");
}

#[tokio::test(flavor = "multi_thread")]
async fn servfail_on_the_x_name_fails_too() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    let r = healthy_resolver().await;
    r.set(
        &format!("x1.{SEED}"),
        RecordType::A,
        Reply::Rcode(ResponseCode::ServFail),
    );
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s2 = step(&checks, 2);
    assert_eq!(s2.status, Status::Fail);
    let fix = s2.fix.as_deref().unwrap();
    assert!(
        fix.contains("cannot get an answer from your nameserver"),
        "{fix}"
    );
    assert!(fix.contains("UDP and TCP 53"), "{fix}");
    assert!(!fix.contains("publish"), "{fix}");
}

#[tokio::test(flavor = "multi_thread")]
async fn refused_advises_another_resolver() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    let r = healthy_resolver().await;
    r.set(
        &format!("x1.{SEED}"),
        RecordType::A,
        Reply::Rcode(ResponseCode::Refused),
    );
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s2 = step(&checks, 2);
    assert_eq!(s2.status, Status::Fail);
    let fix = s2.fix.as_deref().unwrap();
    assert!(
        fix.contains("refuses the query; try another with --resolver"),
        "{fix}"
    );
    assert!(!fix.contains("publish"), "{fix}");
}

#[tokio::test(flavor = "multi_thread")]
async fn ns_nodata_and_cname_get_their_own_wording() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    let r = healthy_resolver().await;
    r.answers(SEED, RecordType::NS, vec![]);
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    assert!(
        step(&checks, 1)
            .detail
            .contains(&format!("no NS record for {SEED} (NODATA)"))
    );
    r.answers(
        SEED,
        RecordType::NS,
        vec![RData::CNAME(hickory_proto::rr::rdata::CNAME(
            Name::from_ascii("elsewhere.example.").unwrap(),
        ))],
    );
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let d = &step(&checks, 1).detail;
    assert!(
        d.contains(&format!("{SEED} is a CNAME to elsewhere.example")),
        "{d}"
    );
    assert!(d.contains("must be delegated with NS"), "{d}");
}

#[tokio::test(flavor = "multi_thread")]
async fn extra_delegations_pass_step_1_with_a_note() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    let r = healthy_resolver().await;
    r.answers(
        SEED,
        RecordType::NS,
        vec![ns("seed-ns.osmium.space."), ns("old-ns.example.")],
    );
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s1 = step(&checks, 1);
    assert_eq!(s1.status, Status::Pass);
    assert!(
        s1.detail.contains("also delegated to old-ns.example")
            && s1.detail.contains("fine if it serves the same zone"),
        "{}",
        s1.detail
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn forged_and_malformed_datagrams_are_ignored() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    fx.write_log(SEED, 1, now());
    let r = healthy_resolver().await;
    r.be_noisy();
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    for n in 1..=4 {
        assert_eq!(step(&checks, n).status, Status::Pass, "{checks:#?}");
    }
    assert!(!step(&checks, 3).detail.contains("203.0.113.99"));
}

#[tokio::test(flavor = "multi_thread")]
async fn answers_that_are_not_nodes_fail_step_3() {
    let fx = Fixture::authoritative(not_a_node());
    let r = healthy_resolver().await;
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    assert_eq!(step(&checks, 2).status, Status::Pass, "{checks:#?}");
    let s3 = step(&checks, 3);
    assert_eq!(s3.status, Status::Fail, "{checks:#?}");
    assert!(s3.fix.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn query_log_absent_fails_step_4() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    let r = healthy_resolver().await;
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s4 = step(&checks, 4);
    assert_eq!(s4.status, Status::Fail);
    assert!(s4.detail.contains("no query log yet at"), "{}", s4.detail);
    assert!(s4.detail.contains(&fx.log_path().display().to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_query_log_fails_step_4() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    fx.write_log(&format!("x1.{SEED}"), 9, now() - 25 * 3600);
    let r = healthy_resolver().await;
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s4 = step(&checks, 4);
    assert_eq!(s4.status, Status::Fail);
    assert!(s4.fix.as_deref().unwrap().contains("delegation"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_query_log_with_no_answered_queries_fails_step_4() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    fx.write_log(&format!("x1.{SEED}"), 0, now() - 60);
    let r = healthy_resolver().await;
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s4 = step(&checks, 4);
    assert_eq!(s4.status, Status::Fail, "{s4:?}");
    assert!(s4.fix.as_deref().unwrap().contains("delegation"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_query_log_two_hours_old_passes_step_4() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    fx.write_log(&format!("x1.{SEED}"), 4, now() - 2 * 3600);
    let r = healthy_resolver().await;
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s4 = step(&checks, 4);
    assert_eq!(s4.status, Status::Pass, "{s4:?}");
    assert!(s4.detail.contains("answered 4 time(s)"), "{s4:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_delegation_fails_step_1() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    let r = healthy_resolver().await;
    r.answers(SEED, RecordType::NS, vec![ns("someone-else.example.")]);
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s1 = step(&checks, 1);
    assert_eq!(s1.status, Status::Fail);
    assert!(s1.detail.contains("someone-else.example"), "{}", s1.detail);
    assert!(s1.fix.as_deref().unwrap().contains(NS_NAME));
}

#[tokio::test(flavor = "multi_thread")]
async fn unresolvable_nameserver_fails_step_1() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    let r = healthy_resolver().await;
    r.set(NS_NAME, RecordType::A, Reply::Rcode(ResponseCode::NXDomain));
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s1 = step(&checks, 1);
    assert_eq!(s1.status, Status::Fail);
    assert!(s1.detail.contains(NS_NAME));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_silent_resolver_is_reported_but_does_not_fail_the_check() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    fx.write_log(SEED, 1, now());
    let good = healthy_resolver().await;
    let silent = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut opts = fx.options(&[&good]);
    opts.resolvers.push(silent.local_addr().unwrap());
    opts.timeout = Duration::from_millis(400);
    let checks = run_checks(&fx.net, &fx.preset, &opts).await;
    let s2 = step(&checks, 2);
    assert_eq!(s2.status, Status::Pass, "{checks:#?}");
    assert!(
        s2.detail
            .contains(&silent.local_addr().unwrap().to_string()),
        "names the resolver that timed out: {}",
        s2.detail
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn all_resolvers_silent_fails() {
    let fx = Fixture::authoritative(free_port());
    let silent = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let opts = DoctorOptions {
        resolvers: vec![silent.local_addr().unwrap()],
        timeout: Duration::from_millis(300),
        probe_max: 5,
    };
    let checks = run_checks(&fx.net, &fx.preset, &opts).await;
    assert_eq!(step(&checks, 1).status, Status::Fail);
    assert_eq!(step(&checks, 2).status, Status::Fail);
    assert_eq!(step(&checks, 3).status, Status::Skip);
}

#[tokio::test(flavor = "multi_thread")]
async fn one_resolver_disagreeing_fails_the_check() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    let good = healthy_resolver().await;
    let bad = healthy_resolver().await;
    bad.set(
        &format!("x1.{SEED}"),
        RecordType::A,
        Reply::Rcode(ResponseCode::NXDomain),
    );
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&good, &bad])).await;
    let s2 = step(&checks, 2);
    assert_eq!(s2.status, Status::Fail);
    assert!(s2.fix.as_deref().unwrap().contains(&bad.addr.to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_truncated_udp_answer_is_retried_over_tcp() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    let r = FakeResolver::start_with(true).await;
    r.answers(SEED, RecordType::NS, vec![ns("seed-ns.osmium.space.")])
        .answers(NS_NAME, RecordType::A, vec![a("192.0.2.53")])
        .answers(SEED, RecordType::A, vec![a("127.0.0.1")])
        .answers(&format!("x1.{SEED}"), RecordType::A, vec![a("127.0.0.1")])
        .answers(SEED, RecordType::AAAA, vec![])
        .answers(&format!("x1.{SEED}"), RecordType::AAAA, vec![]);
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    assert_eq!(step(&checks, 2).status, Status::Pass, "{checks:#?}");
    assert!(r.seen().iter().any(|s| s.tcp));
}

fn seed_records(cf: &FakeCloudflare) {
    for name in [SEED.to_string(), format!("x1.{SEED}"), format!("x5.{SEED}")] {
        cf.seed(FakeRecord::new(
            &name,
            "A",
            "127.0.0.1",
            Some(OWNER_COMMENT),
        ));
    }
}

const HOUR: &str = "2026-10-25T15:00:00Z";

/// Steps 1 to 3 of a healthy Cloudflare seed, then the analytics the caller set up.
async fn cloudflare_step4(setup: impl FnOnce(&FakeCloudflare)) -> Vec<Check> {
    let (peer, _h) = peer_for(70231);
    let cf = FakeCloudflare::start("zone1", "secret-token").await;
    seed_records(&cf);
    setup(&cf);
    let fx = Fixture::cloudflare(peer.port(), &cf.base_url(), "zone1");
    let r = healthy_resolver().await;
    run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_analytics_for_the_x_name_pass_step_4() {
    let checks = cloudflare_step4(|cf| {
        cf.set_analytics_rows(&[
            (&format!("x1.{SEED}"), HOUR, 20),
            (&format!("x1.{SEED}"), "2026-10-25T11:00:00Z", 20),
            (SEED, HOUR, 120),
        ]);
    })
    .await;
    for n in 1..=3 {
        assert_eq!(step(&checks, n).status, Status::Pass, "{checks:#?}");
    }
    let s4 = step(&checks, 4);
    assert_eq!(s4.status, Status::Pass, "{s4:?}");
    assert_eq!(
        s4.detail,
        format!(
            "x1.{SEED} ~40 queries, last 10-25 15:00 UTC; {SEED} ~120 queries, last 10-25 15:00 UTC \
             (sampled; includes your own lookups)"
        )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_analytics_for_the_plain_name_alone_pass() {
    let checks = cloudflare_step4(|cf| cf.set_analytics_rows(&[(SEED, HOUR, 10)])).await;
    let s4 = step(&checks, 4);
    assert_eq!(s4.status, Status::Pass, "{checks:#?}");
    assert_eq!(
        s4.detail,
        format!(
            "x1.{SEED} ~0 queries; {SEED} ~10 queries, last 10-25 15:00 UTC \
             (sampled; includes your own lookups)"
        )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_partial_errors_with_rows_pass_and_are_reported() {
    let checks = cloudflare_step4(|cf| {
        cf.set_analytics_rows(&[(SEED, HOUR, 10)]);
        cf.set_analytics_partial_error("sampling degraded");
    })
    .await;
    let s4 = step(&checks, 4);
    assert_eq!(s4.status, Status::Pass, "{s4:?}");
    assert!(
        s4.detail
            .ends_with("(Cloudflare also reported: sampling degraded)"),
        "{s4:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_partial_errors_without_rows_skip() {
    let checks = cloudflare_step4(|cf| cf.set_analytics_partial_error("sampling degraded")).await;
    let s4 = step(&checks, 4);
    assert_eq!(s4.status, Status::Skip, "{s4:?}");
    assert!(s4.detail.contains("sampling degraded"), "{s4:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_token_blind_to_the_zone_skips_step_4() {
    let checks = cloudflare_step4(|cf| cf.set_analytics_no_zones()).await;
    let s4 = step(&checks, 4);
    assert_eq!(s4.status, Status::Skip, "{s4:?}");
    assert!(s4.detail.contains("cannot see the zone"), "{s4:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_analytics_for_unrelated_names_fail_step_4() {
    let checks = cloudflare_step4(|cf| {
        cf.set_analytics_rows(&[
            ("other.example.org", HOUR, 50),
            (&format!("x5.{SEED}"), HOUR, 50),
        ]);
    })
    .await;
    let s4 = step(&checks, 4);
    assert_eq!(s4.status, Status::Fail, "{s4:?}");
    assert!(s4.fix.as_deref().unwrap().contains("no resolver has asked"));
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_zero_analytics_rows_fail_step_4() {
    let checks = cloudflare_step4(|_| {}).await;
    assert_eq!(step(&checks, 4).status, Status::Fail, "{checks:#?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_analytics_rows_with_zero_counts_fail_step_4() {
    let checks = cloudflare_step4(|cf| {
        cf.set_analytics_rows(&[
            (&format!("x1.{SEED}"), HOUR, 0),
            (SEED, HOUR, 0),
            (&format!("x5.{SEED}"), HOUR, 0),
        ]);
    })
    .await;
    let s4 = step(&checks, 4);
    assert_eq!(s4.status, Status::Fail, "{s4:?}");
    assert!(s4.fix.as_deref().unwrap().contains("no resolver has asked"));
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_analytics_errors_skip_step_4_naming_the_permission() {
    let checks = cloudflare_step4(|cf| cf.set_analytics_error("not authorized")).await;
    let s4 = step(&checks, 4);
    assert_eq!(s4.status, Status::Skip, "{s4:?}");
    assert!(s4.detail.contains("not authorized"), "{s4:?}");
    assert!(
        s4.detail.contains("Zone \u{2192} Analytics \u{2192} Read"),
        "{s4:?}"
    );
    assert!(!format!("{s4:?}").contains("secret-token"));
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_foreign_records_fail_step_1() {
    let (peer, _h) = peer_for(70231);
    let cf = FakeCloudflare::start("zone1", "secret-token").await;
    seed_records(&cf);
    cf.seed(FakeRecord::new(
        &format!("x1.{SEED}"),
        "A",
        "203.0.113.9",
        Some("hand made"),
    ));
    let fx = Fixture::cloudflare(peer.port(), &cf.base_url(), "zone1");
    let r = healthy_resolver().await;
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s1 = step(&checks, 1);
    assert_eq!(s1.status, Status::Fail, "{checks:#?}");
    assert!(s1.detail.contains("203.0.113.9"), "{}", s1.detail);
    assert!(
        s1.fix
            .as_deref()
            .unwrap()
            .contains("delete them in the dashboard")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_without_owned_records_fails_step_1() {
    let cf = FakeCloudflare::start("zone1", "secret-token").await;
    let fx = Fixture::cloudflare(free_port(), &cf.base_url(), "zone1");
    let r = healthy_resolver().await;
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s1 = step(&checks, 1);
    assert_eq!(s1.status, Status::Fail);
    assert!(
        s1.fix
            .as_deref()
            .unwrap()
            .contains("is the seed name inside this zone, and has the seeder published yet?")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_bad_token_fails_step_1_without_printing_it() {
    let cf = FakeCloudflare::start("zone1", "another-token").await;
    let fx = Fixture::cloudflare(free_port(), &cf.base_url(), "zone1");
    let r = healthy_resolver().await;
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s1 = step(&checks, 1);
    assert_eq!(s1.status, Status::Fail);
    let text = format!("{checks:?}");
    assert!(!text.contains("secret-token"));
    assert!(!text.contains("another-token"));
}

#[tokio::test(flavor = "multi_thread")]
async fn cloudflare_wrong_zone_id_fails_step_1_with_the_fix() {
    let cf = FakeCloudflare::start("zone1", "secret-token").await;
    let fx = Fixture::cloudflare(free_port(), &cf.base_url(), "wrong-zone");
    let r = healthy_resolver().await;
    let checks = run_checks(&fx.net, &fx.preset, &fx.options(&[&r])).await;
    let s1 = step(&checks, 1);
    assert_eq!(s1.status, Status::Fail, "{checks:#?}");
    assert!(
        s1.detail.contains("zone_id was not found or is malformed"),
        "{}",
        s1.detail
    );
    assert!(s1.fix.as_deref().unwrap().contains("Overview page"));
}

fn doctor_cmd(cfg: &Path, resolvers: &[&FakeResolver]) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_seedcrystal"));
    cmd.arg("doctor").arg("--config").arg(cfg);
    for r in resolvers {
        cmd.arg("--resolver").arg(r.addr.to_string());
    }
    cmd.env_remove("SEEDCRYSTAL_CLOUDFLARE_TOKEN")
        .output()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn exit_code_is_1_on_any_fail() {
    let fx = Fixture::authoritative(free_port());
    let r = FakeResolver::start().await; // everything NXDOMAIN
    let cfg = fx.cfg_path.clone();
    let out = tokio::task::spawn_blocking(move || {
        let o = doctor_cmd(&cfg, &[&r]);
        drop(r);
        o
    })
    .await
    .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("FAIL"), "{text}");
    assert!(text.contains("  fix: "), "{text}");
    drop(fx.dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn exit_code_is_0_when_every_check_passes_or_skips() {
    let (peer, _h) = peer_for(70231);
    let fx = Fixture::authoritative(peer.port());
    fx.write_log(&format!("x1.{SEED}"), 2, now());
    let r = healthy_resolver().await;
    let cfg = fx.cfg_path.clone();
    let out = tokio::task::spawn_blocking(move || {
        let o = doctor_cmd(&cfg, &[&r]);
        drop(r);
        o
    })
    .await
    .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(text.contains("PASS  1  "), "{text}");
}

#[test]
fn resolver_flag_rejects_garbage() {
    let out = Command::new(env!("CARGO_BIN_EXE_seedcrystal"))
        .args([
            "doctor",
            "--config",
            "/nonexistent",
            "--resolver",
            "nonsense",
        ])
        .env_remove("SEEDCRYSTAL_CLOUDFLARE_TOKEN")
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("resolver"));
}
