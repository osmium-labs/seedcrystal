//! Live checks of a preset against the coin's real network: level 2 (handshake) and level 3
//! (a short crawl). Both are `#[ignore]` and driven by environment variables; CI runs only the
//! offline tests of the pass/fail rules.
//!
//! ```text
//! SEEDCRYSTAL_LIVE_PRESET=dash-mainnet cargo test --test live_presets level2 -- --ignored --nocapture
//! SEEDCRYSTAL_LIVE_PRESET=dash-mainnet cargo test --test live_presets level3 -- --ignored --nocapture
//! ```
//!
//! `SEEDCRYSTAL_LIVE_MAX` caps the level 2 probes (default 5, at most 5), and
//! `SEEDCRYSTAL_LIVE_MINUTES` the level 3 crawl (default 10, at most 10). Nothing retries.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use seedcrystal::coin::{CoinPreset, builtin};
use seedcrystal::crawler::{CrawlerConfig, policy_for, spawn};
use seedcrystal::dns::answer::{Family, Target, matching};
use seedcrystal::p2p::{ChainCheck, ProbeError, ProbeOptions, probe};
use seedcrystal::store::NodeStore;
use tokio_util::sync::CancellationToken;

/// The client prefix the coin's own nodes put in their user agent.
fn reference_agent(preset_id: &str) -> Option<&'static str> {
    // `bitcoincash-` before `bitcoin-`, which is its prefix.
    const CLIENTS: &[(&str, &str)] = &[
        ("dash-", "/Dash Core:"),
        ("litecoin-", "/LitecoinCore:"),
        ("dogecoin-", "/Shibetoshi:"),
        ("bitcoincash-", "/Bitcoin Cash Node:"),
        ("ecash-", "/Bitcoin ABC:"),
        ("pivx-", "/PIVX Core:"),
        ("bitcoin-", "/Satoshi:"),
        ("osmium-", "/Osmium Core:"),
        ("raptoreum-", "/Raptoreum Core:"),
    ];
    CLIENTS
        .iter()
        .find(|(prefix, _)| preset_id.starts_with(prefix))
        .map(|(_, client)| *client)
}

#[derive(Debug)]
enum PeerResult {
    Ok {
        version: i32,
        agent: String,
        services: u64,
        height: i32,
        addrs: usize,
        chain: ChainCheck,
    },
    /// No TCP connection, or no answer in time: the node may simply be offline.
    Unreachable(String),
    /// The node answered and the handshake failed.
    Failed(String),
}

/// The level 2 failures; empty means pass.
fn judge_handshakes(
    peers: &[(SocketAddr, PeerResult)],
    testnet: bool,
    checkpoint: bool,
    reference: &str,
) -> Vec<String> {
    if peers.is_empty() {
        return vec!["no addresses to probe".to_string()];
    }
    let mut fails = Vec::new();
    let ok: Vec<_> = peers
        .iter()
        .filter_map(|(addr, r)| match r {
            PeerResult::Ok {
                agent,
                addrs,
                chain,
                ..
            } => Some((addr, agent, *addrs, *chain)),
            _ => None,
        })
        .collect();
    if testnet {
        let reachable = peers
            .iter()
            .filter(|(_, r)| !matches!(r, PeerResult::Unreachable(_)))
            .count();
        if ok.is_empty() || ok.len() < reachable {
            fails.push(format!(
                "{} of {reachable} reachable testnet nodes completed the handshake (need all, and at least 1)",
                ok.len()
            ));
        }
    } else if ok.len() < 3 {
        fails.push(format!(
            "{} of {} completed the handshake (need 3)",
            ok.len(),
            peers.len()
        ));
    }
    if !ok.iter().any(|(_, _, addrs, _)| *addrs > 0) {
        fails.push("no peer returned addresses".to_string());
    }
    if checkpoint {
        if !ok.iter().any(|(_, _, _, c)| *c == ChainCheck::Verified) {
            fails.push("no peer was Verified against the checkpoint".to_string());
        }
        for (addr, agent, _, chain) in &ok {
            if *chain == ChainCheck::WrongChain && agent.starts_with(reference) {
                fails.push(format!(
                    "{addr} runs {agent} but is on the wrong chain: the checkpoint is wrong"
                ));
            }
        }
    }
    fails
}

#[derive(Debug, Clone)]
struct CrawlReport {
    served: usize,
    served_agents: Histogram<String>,
    services: Histogram<u64>,
    tip: i32,
    verified: u64,
    wrong: u64,
    warnings: Vec<String>,
}

/// The level 3 failures; empty means pass. The tip is checked by hand against an explorer.
fn judge_crawl(r: &CrawlReport, mainnet: bool, checkpoint: bool, reference: &str) -> Vec<String> {
    let mut fails = Vec::new();
    if mainnet && r.served < 10 {
        fails.push(format!(
            "{} served nodes match seed_flags (need 10)",
            r.served
        ));
    }
    match r.served_agents.first() {
        Some((agent, _)) if agent.starts_with(reference) => {}
        Some((agent, n)) => fails.push(format!(
            "the top served agent is {agent} ({n} nodes), not {reference}"
        )),
        None => fails.push("no served node has a user agent".to_string()),
    }
    if checkpoint && r.verified <= r.wrong {
        fails.push(format!(
            "{} verified against {} wrong-chain nodes",
            r.verified, r.wrong
        ));
    }
    for w in &r.warnings {
        // A dead bootstrap name is the coin's problem, not the preset's.
        if !w.contains("bootstrap lookup failed") && !w.contains("bootstrap lookup timed out") {
            fails.push(format!("warning: {w}"));
        }
    }
    fails
}

fn ok(agent: &str, addrs: usize, chain: ChainCheck) -> PeerResult {
    PeerResult::Ok {
        version: 70016,
        agent: agent.to_string(),
        services: 0x9,
        height: 100,
        addrs,
        chain,
    }
}

fn at(i: u8) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, i], 8333))
}

fn peers(results: Vec<PeerResult>) -> Vec<(SocketAddr, PeerResult)> {
    results
        .into_iter()
        .enumerate()
        .map(|(i, r)| (at(i as u8 + 1), r))
        .collect()
}

const BCHN: &str = "/Bitcoin Cash Node:";

#[test]
fn handshakes_pass_with_three_ok_and_addresses() {
    let p = peers(vec![
        ok("/Satoshi:27.0.0/", 0, ChainCheck::NotChecked),
        ok("/Satoshi:27.0.0/", 900, ChainCheck::NotChecked),
        ok("/Satoshi:28.0.0/", 0, ChainCheck::NotChecked),
        PeerResult::Failed("peer closed the connection".into()),
    ]);
    assert_eq!(
        judge_handshakes(&p, false, false, "/Satoshi:"),
        Vec::<String>::new()
    );
}

#[test]
fn handshakes_fail_without_addresses_or_with_too_few_ok() {
    let two = peers(vec![
        ok("/Satoshi:27.0.0/", 900, ChainCheck::NotChecked),
        ok("/Satoshi:27.0.0/", 900, ChainCheck::NotChecked),
        PeerResult::Failed("peer protocol version 70001 is below the floor".into()),
    ]);
    let fails = judge_handshakes(&two, false, false, "/Satoshi:");
    assert_eq!(fails.len(), 1, "{fails:?}");
    assert!(fails[0].contains("2 of 3"), "{fails:?}");

    let silent = peers(vec![
        ok("/Satoshi:27.0.0/", 0, ChainCheck::NotChecked),
        ok("/Satoshi:27.0.0/", 0, ChainCheck::NotChecked),
        ok("/Satoshi:27.0.0/", 0, ChainCheck::NotChecked),
    ]);
    let fails = judge_handshakes(&silent, false, false, "/Satoshi:");
    assert_eq!(fails.len(), 1, "{fails:?}");
    assert!(fails[0].contains("addresses"), "{fails:?}");
}

#[test]
fn handshakes_empty_list_fails() {
    assert_eq!(
        judge_handshakes(&[], false, false, "/Satoshi:"),
        vec!["no addresses to probe".to_string()]
    );
    assert_eq!(
        judge_handshakes(&[], true, false, "/Satoshi:"),
        vec!["no addresses to probe".to_string()]
    );
}

#[test]
fn testnet_passes_with_one_reachable_node() {
    let p = peers(vec![
        ok("/Satoshi:27.0.0/", 10, ChainCheck::NotChecked),
        PeerResult::Unreachable("connect failed: Connection refused".into()),
        PeerResult::Unreachable("timed out".into()),
    ]);
    assert_eq!(
        judge_handshakes(&p, true, false, "/Satoshi:"),
        Vec::<String>::new()
    );
    // A reachable testnet node that refuses our handshake is a failure.
    let p = peers(vec![
        ok("/Satoshi:27.0.0/", 10, ChainCheck::NotChecked),
        PeerResult::Failed("peer closed the connection".into()),
    ]);
    assert_eq!(judge_handshakes(&p, true, false, "/Satoshi:").len(), 1);
    // Unreachable nodes alone are no pass.
    let p = peers(vec![PeerResult::Unreachable("timed out".into())]);
    assert_eq!(judge_handshakes(&p, true, false, "/Satoshi:").len(), 2);
}

#[test]
fn checkpoint_needs_a_verified_peer_and_rejects_wrong_chain_reference_clients() {
    let fine = peers(vec![
        ok("/Bitcoin Cash Node:29.2.0/", 900, ChainCheck::Verified),
        ok("/Bitcoin Cash Node:29.2.0/", 0, ChainCheck::Verified),
        ok("/Bitcoin ABC:0.34.3/", 0, ChainCheck::WrongChain),
    ]);
    assert_eq!(
        judge_handshakes(&fine, false, true, BCHN),
        Vec::<String>::new()
    );

    let none_verified = peers(vec![
        ok("/Bitcoin ABC:0.34.3/", 900, ChainCheck::WrongChain),
        ok("/Bitcoin SV:1.1.0/", 0, ChainCheck::WrongChain),
        ok("/Bitcoin Cash Node:29.2.0/", 0, ChainCheck::Unverified),
    ]);
    let fails = judge_handshakes(&none_verified, false, true, BCHN);
    assert_eq!(fails.len(), 1, "{fails:?}");
    assert!(fails[0].contains("Verified"), "{fails:?}");

    let own_client_wrong = peers(vec![
        ok("/Bitcoin Cash Node:29.2.0/", 900, ChainCheck::Verified),
        ok("/Bitcoin Cash Node:29.2.0/", 0, ChainCheck::Verified),
        ok("/Bitcoin Cash Node:29.2.0/", 0, ChainCheck::WrongChain),
    ]);
    let fails = judge_handshakes(&own_client_wrong, false, true, BCHN);
    assert_eq!(fails.len(), 1, "{fails:?}");
    assert!(fails[0].contains("10.0.0.3:8333"), "{fails:?}");
}

fn report() -> CrawlReport {
    CrawlReport {
        served: 40,
        served_agents: vec![
            ("/Bitcoin Cash Node:29.2.0/".into(), 30),
            ("/Bitcoin Cash Node:28.0.0/".into(), 10),
        ],
        services: vec![(0x25, 40)],
        tip: 900_000,
        verified: 60,
        wrong: 20,
        warnings: Vec::new(),
    }
}

#[test]
fn a_good_crawl_passes() {
    assert_eq!(
        judge_crawl(&report(), true, true, BCHN),
        Vec::<String>::new()
    );
}

#[test]
fn crawl_fails_when_no_served_node_matches() {
    let r = CrawlReport {
        served: 0,
        served_agents: Vec::new(),
        services: Vec::new(),
        verified: 20,
        wrong: 0,
        ..report()
    };
    let fails = judge_crawl(&r, true, true, BCHN);
    assert!(fails.iter().any(|f| f.contains("served")), "{fails:?}");
}

#[test]
fn crawl_fails_on_a_foreign_top_agent() {
    let r = CrawlReport {
        served_agents: vec![
            ("/Bitcoin ABC:0.34.3/".into(), 30),
            ("/Bitcoin Cash Node:29.2.0/".into(), 10),
        ],
        ..report()
    };
    let fails = judge_crawl(&r, true, true, BCHN);
    assert_eq!(fails.len(), 1, "{fails:?}");
    assert!(fails[0].contains("/Bitcoin ABC:0.34.3/"), "{fails:?}");
}

#[test]
fn crawl_fails_on_wrong_majority() {
    let r = CrawlReport {
        verified: 20,
        wrong: 20,
        ..report()
    };
    assert_eq!(judge_crawl(&r, true, true, BCHN).len(), 1);
    // Without a checkpoint the verdict counts are not judged.
    assert_eq!(judge_crawl(&r, true, false, BCHN), Vec::<String>::new());
}

#[test]
fn crawl_ignores_bootstrap_lookup_failures_only() {
    let r = CrawlReport {
        warnings: vec![
            "WARN seedcrystal::crawler: bootstrap lookup failed name=seed.dead.example".into(),
            "WARN seedcrystal::crawler: bootstrap lookup timed out name=seed.slow.example".into(),
        ],
        ..report()
    };
    assert_eq!(judge_crawl(&r, true, true, BCHN), Vec::<String>::new());

    let r = CrawlReport {
        warnings: vec!["WARN seedcrystal::crawler: census: writing census.json".into()],
        ..report()
    };
    let fails = judge_crawl(&r, true, true, BCHN);
    assert_eq!(fails.len(), 1, "{fails:?}");
    assert!(fails[0].contains("census:"), "{fails:?}");
}

#[test]
fn testnet_crawl_needs_no_served_minimum() {
    let r = CrawlReport {
        served: 2,
        served_agents: vec![("/Bitcoin Cash Node:29.2.0/".into(), 2)],
        ..report()
    };
    assert_eq!(judge_crawl(&r, false, true, BCHN), Vec::<String>::new());
    assert_eq!(judge_crawl(&r, true, true, BCHN).len(), 1);
}

#[test]
fn reference_agents_cover_every_builtin_family() {
    let ids = [
        "bitcoin-mainnet",
        "bitcoin-testnet4",
        "osmium-mainnet",
        "osmium-testnet",
        "raptoreum-mainnet",
        "raptoreum-testnet",
    ];
    for id in ids {
        assert!(
            reference_agent(id).is_some(),
            "{id} has no reference client"
        );
    }
    for id in seedcrystal::coin::builtin_ids() {
        assert!(ids.contains(&id), "{id} is built in but not listed here");
    }
    assert_eq!(reference_agent("bitcoincash-mainnet"), Some(BCHN));
    assert_eq!(reference_agent("bitcoin-mainnet"), Some("/Satoshi:"));
    assert_eq!(reference_agent("unknown-mainnet"), None);
}

fn live_preset() -> CoinPreset {
    let id = std::env::var("SEEDCRYSTAL_LIVE_PRESET").expect("set SEEDCRYSTAL_LIVE_PRESET=<id>");
    builtin(&id).unwrap()
}

fn env_clamped(name: &str, default: u64, max: u64) -> u64 {
    std::env::var(name)
        .ok()
        .map(|v| {
            v.parse::<u64>()
                .unwrap_or_else(|_| panic!("{name} must be a number"))
        })
        .unwrap_or(default)
        .clamp(1, max)
}

fn is_mainnet(preset: &CoinPreset) -> bool {
    preset.id.ends_with("-mainnet")
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "probes the real network; see the module docs"]
async fn level2() {
    let preset = live_preset();
    let max = env_clamped("SEEDCRYSTAL_LIVE_MAX", 5, 5) as usize;
    let mut targets: Vec<SocketAddr> = Vec::new();
    for name in &preset.bootstrap_dns {
        match tokio::net::lookup_host((name.as_str(), preset.default_port)).await {
            Ok(addrs) => {
                let addrs: Vec<_> = addrs.collect();
                println!("{name}: {} addresses", addrs.len());
                targets.extend(addrs);
            }
            Err(e) => println!("{name}: lookup failed: {e}"),
        }
    }
    targets.extend(preset.bootstrap_fixed.iter().copied());
    let mut seen = std::collections::HashSet::new();
    targets.retain(|a| seen.insert(*a));
    targets.truncate(max);

    let opts = ProbeOptions {
        want_addrs: true,
        check_chain: preset.checkpoint.is_some(),
        ..ProbeOptions::default()
    };
    let mut peers = Vec::new();
    for target in targets {
        let result = match probe(target, &preset, &opts).await {
            Ok(r) => PeerResult::Ok {
                version: r.peer.version,
                agent: r.peer.user_agent.clone(),
                services: r.peer.services,
                height: r.peer.start_height,
                addrs: r.addrs.len(),
                chain: r.chain,
            },
            Err(e @ (ProbeError::Connect(_) | ProbeError::Timeout)) => {
                PeerResult::Unreachable(e.to_string())
            }
            Err(e) => PeerResult::Failed(e.to_string()),
        };
        match &result {
            PeerResult::Ok {
                version,
                agent,
                services,
                height,
                addrs,
                chain,
            } => println!(
                "{target}: ok version {version} agent {agent:?} services {services:#x} height {height} addrs {addrs} chain {chain:?}"
            ),
            PeerResult::Unreachable(e) => println!("{target}: unreachable: {e}"),
            PeerResult::Failed(e) => println!("{target}: FAILED: {e}"),
        }
        peers.push((target, result));
    }

    let reference = reference_agent(&preset.id).expect("no reference client for this preset");
    let fails = judge_handshakes(
        &peers,
        !is_mainnet(&preset),
        preset.checkpoint.is_some(),
        reference,
    );
    println!("level 2 {}: {fails:?}", preset.id);
    assert!(fails.is_empty(), "level 2 failed: {fails:?}");
}

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = Logs;
    fn make_writer(&'a self) -> Logs {
        self.clone()
    }
}

/// Values with their counts, most common first.
type Histogram<T> = Vec<(T, usize)>;

/// Agents and service bits of the served addresses, and the store's verdict counts.
fn read_store(path: &Path, served: &[SocketAddr]) -> (Histogram<String>, Histogram<u64>, u64, u64) {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut agents: HashMap<String, usize> = HashMap::new();
    let mut services: HashMap<u64, usize> = HashMap::new();
    for addr in served {
        let (agent, bits): (String, i64) = conn
            .query_row(
                "SELECT user_agent, services FROM nodes WHERE ip = ?1 AND port = ?2",
                rusqlite::params![addr.ip().to_string(), addr.port()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        *agents.entry(agent).or_default() += 1;
        *services.entry(bits as u64).or_default() += 1;
    }
    let mut agents: Vec<_> = agents.into_iter().collect();
    agents.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let mut services: Vec<_> = services.into_iter().collect();
    services.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let (mut verified, mut wrong) = (0, 0);
    let mut stmt = conn
        .prepare("SELECT chain, chain_ref, COUNT(*) FROM nodes GROUP BY chain, chain_ref")
        .unwrap();
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, i64>(2)? as u64,
            ))
        })
        .unwrap();
    for row in rows {
        let (chain, chain_ref, n) = row.unwrap();
        println!("verdict chain={chain} ref={chain_ref:?}: {n}");
        match chain {
            1 => verified += n,
            2 => wrong += n,
            _ => {}
        }
    }
    (agents, services, verified, wrong)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "crawls the real network; see the module docs"]
async fn level3() {
    let preset = live_preset();
    let minutes = env_clamped("SEEDCRYSTAL_LIVE_MINUTES", 10, 10);
    let logs = Logs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(logs.clone())
        .with_ansi(false)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);

    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("n.db");
    let mut cfg = CrawlerConfig::new(preset.clone(), store_path.clone());
    cfg.concurrency = 16;
    cfg.allow_private = false;
    cfg.census_path = Some(dir.path().join("census.json"));
    let token = CancellationToken::new();
    let (rx, handle) = spawn(cfg, token.clone()).unwrap();
    tokio::time::sleep(Duration::from_secs(minutes * 60)).await;
    let snapshot = rx.borrow().clone();
    token.cancel();
    handle.await.unwrap().unwrap();
    drop(guard);

    let mut served: Vec<SocketAddr> = Vec::new();
    for family in [Family::V4, Family::V6] {
        let ips = matching(&snapshot.nodes, &Target::Plain, preset.seed_flags, family);
        served.extend(
            snapshot
                .nodes
                .iter()
                .map(|n| n.addr)
                .filter(|a| ips.contains(&a.ip())),
        );
    }
    let (served_agents, services, verified, wrong) = read_store(&store_path, &served);
    let now = unix_now();
    let tip = NodeStore::open_existing(&store_path)
        .unwrap()
        .tip(&policy_for(&preset, false), now)
        .unwrap();
    let warnings: Vec<String> = String::from_utf8(logs.0.lock().unwrap().clone())
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(str::to_string)
        .collect();
    let report = CrawlReport {
        served: served.len(),
        served_agents,
        services,
        tip,
        verified,
        wrong,
        warnings,
    };

    println!("level 3 {} after {minutes} min", preset.id);
    println!(
        "snapshot nodes: {}, served for seed_flags {:#x}: {}",
        snapshot.nodes.len(),
        preset.seed_flags,
        report.served
    );
    for (bits, n) in &report.services {
        println!("  services {bits:#x}: {n}");
    }
    for (agent, n) in report.served_agents.iter().take(10) {
        println!("  agent {agent:?}: {n}");
    }
    println!("verified {}, wrong chain {}", report.verified, report.wrong);
    println!(
        "store tip {} (explorer tip: check by hand; tolerance {} blocks)",
        report.tip, preset.tip_tolerance
    );
    for w in &report.warnings {
        println!("  {w}");
    }
    let reference = reference_agent(&preset.id).expect("no reference client for this preset");
    let fails = judge_crawl(
        &report,
        is_mainnet(&preset),
        preset.checkpoint.is_some(),
        reference,
    );
    println!("level 3 {}: {fails:?}", preset.id);
    assert!(fails.is_empty(), "level 3 failed: {fails:?}");
}
