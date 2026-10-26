mod support;

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use seedcrystal::coin::{Checkpoint, CoinPreset, builtin};
use seedcrystal::crawler::{CrawlerConfig, Snapshot, spawn};
use seedcrystal::p2p::{AddrEntry, ChainCheck, NetAddr, ProbeOptions, encode_addr};
use seedcrystal::store::{NodeStore, Outcome};
use support::{FakePeer, FakeScript};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

fn preset() -> CoinPreset {
    let mut p = builtin("osmium-mainnet").unwrap();
    p.bootstrap_dns.clear();
    p.bootstrap_fixed.clear();
    p
}

fn config(path: &Path, bootstrap: Vec<SocketAddr>) -> CrawlerConfig {
    CrawlerConfig {
        preset: preset(),
        store_path: path.to_path_buf(),
        bootstrap,
        concurrency: 8,
        snapshot_interval: Duration::from_millis(200),
        allow_private: true,
        bootstrap_retry: Duration::from_millis(300),
        bootstrap_source: None,
        probe: ProbeOptions {
            connect_timeout: Duration::from_secs(2),
            handshake_timeout: Duration::from_secs(3),
            addr_wait: Duration::from_millis(500),
            ..ProbeOptions::default()
        },
    }
}

fn fake(addr_reply: Vec<AddrEntry>) -> SocketAddr {
    let p = preset();
    let mut script = FakeScript::new(p.magic, p.protocol_version);
    script.addr_reply = addr_reply;
    FakePeer::spawn(script).0
}

fn entry(addr: SocketAddr) -> AddrEntry {
    let std::net::IpAddr::V4(ip) = addr.ip() else {
        panic!("v4 only")
    };
    AddrEntry {
        addr: NetAddr::V4(ip),
        port: addr.port(),
        services: 1,
        time: 1_700_000_000,
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

async fn bounded<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), f)
        .await
        .expect("test exceeded 20 s")
}

async fn wait_until(rx: &mut watch::Receiver<Arc<Snapshot>>, ok: impl Fn(&Snapshot) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ok(&rx.borrow()) {
        assert!(Instant::now() < deadline, "condition not met within 10 s");
        let _ = tokio::time::timeout(Duration::from_millis(100), rx.changed()).await;
    }
}

fn row_count(path: &Path) -> i64 {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row("SELECT COUNT(*) FROM nodes", [], |r| r.get(0))
        .unwrap()
}

#[tokio::test]
async fn crawls_from_one_bootstrap_to_whole_network() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let b = fake(vec![]);
        let c = fake(vec![]);
        let a = fake(vec![entry(b), entry(c)]);
        let token = CancellationToken::new();
        let (mut rx, handle) =
            spawn(config(&dir.path().join("n.db"), vec![a]), token.clone()).unwrap();
        wait_until(&mut rx, |s| {
            [a, b, c]
                .iter()
                .all(|x| s.nodes.iter().any(|n| n.addr == *x))
        })
        .await;
        token.cancel();
        handle.await.unwrap().unwrap();
    })
    .await;
}

#[tokio::test]
async fn caps_learned_addresses_per_response() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("n.db");
        let many: Vec<AddrEntry> = (0..5000u32)
            .map(|i| AddrEntry {
                addr: NetAddr::V4(Ipv4Addr::new(127, 0, (i >> 8) as u8 + 1, i as u8)),
                port: 20_000 + (i % 1000) as u16,
                services: 1,
                time: 1_700_000_000,
            })
            .collect();
        let p = preset();
        let mut script = FakeScript::new(p.magic, p.protocol_version);
        for chunk in many.chunks(1000) {
            script
                .after_verack
                .push(("addr".to_string(), encode_addr(chunk)));
        }
        let a = FakePeer::spawn(script).0;
        let token = CancellationToken::new();
        let (mut rx, handle) = spawn(config(&path, vec![a]), token.clone()).unwrap();
        wait_until(&mut rx, |s| s.nodes.iter().any(|n| n.addr == a)).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let rows = row_count(&path);
        token.cancel();
        handle.await.unwrap().unwrap();
        assert!(rows > 1, "harvest was not stored ({rows} rows)");
        assert!(rows <= 1001, "{rows} rows stored");
    })
    .await;
}

#[tokio::test]
async fn unreachable_bootstrap_does_not_stop_crawl() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let closed = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        };
        let a = fake(vec![]);
        let token = CancellationToken::new();
        let (mut rx, handle) = spawn(
            config(&dir.path().join("n.db"), vec![closed, a]),
            token.clone(),
        )
        .unwrap();
        wait_until(&mut rx, |s| s.nodes.iter().any(|n| n.addr == a)).await;
        assert!(!rx.borrow().nodes.iter().any(|n| n.addr == closed));
        token.cancel();
        handle.await.unwrap().unwrap();
    })
    .await;
}

#[tokio::test]
async fn first_snapshot_comes_from_store() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("n.db");
        let now = unix_now();
        {
            let store = NodeStore::open(&path).unwrap();
            for i in 1..=3u8 {
                let addr: SocketAddr = format!("127.0.0.{i}:9000").parse().unwrap();
                store.add_learned(&[entry(addr)], now, None).unwrap();
                store
                    .record(
                        addr,
                        Outcome::Good {
                            version: preset().protocol_version,
                            services: 1,
                            height: 100,
                            harvested: false,
                            chain: ChainCheck::NotChecked,
                        },
                        now,
                        None,
                    )
                    .unwrap();
            }
        }
        let token = CancellationToken::new();
        let (rx, handle) = spawn(config(&path, vec![]), token.clone()).unwrap();
        assert_eq!(rx.borrow().nodes.len(), 3);
        token.cancel();
        handle.await.unwrap().unwrap();
    })
    .await;
}

#[tokio::test]
async fn shutdown_stops_within_two_seconds() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("n.db");
        // Accepts and never answers, so a probe is certainly in flight at cancel time.
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let silent_addr = silent.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let holder = tokio::spawn(async move {
            let (stream, _) = silent.accept().await.unwrap();
            let _ = accepted_tx.send(());
            tokio::time::sleep(Duration::from_secs(10)).await;
            drop(stream);
        });
        let token = CancellationToken::new();
        let (_rx, handle) = spawn(
            config(&path, vec![silent_addr, fake(vec![])]),
            token.clone(),
        )
        .unwrap();
        accepted_rx.await.expect("probe never connected");
        let started = Instant::now();
        token.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("crawler did not stop within 2 s")
            .unwrap()
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        // Nothing may still be writing: an immediate write lock must succeed at once.
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.busy_timeout(Duration::ZERO).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE; COMMIT;").unwrap();
        holder.abort();
    })
    .await;
}

#[tokio::test]
async fn bootstrap_is_retried_while_nothing_is_servable() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let a = fake(vec![]);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut cfg = config(&dir.path().join("n.db"), vec![]);
        let seen = Arc::clone(&calls);
        cfg.bootstrap_source = Some(Arc::new(move || {
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                Vec::new()
            } else {
                vec![a]
            }
        }));
        let token = CancellationToken::new();
        let (mut rx, handle) = spawn(cfg, token.clone()).unwrap();
        wait_until(&mut rx, |s| s.nodes.iter().any(|n| n.addr == a)).await;
        assert!(calls.load(Ordering::SeqCst) >= 2);
        token.cancel();
        handle.await.unwrap().unwrap();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_snapshot_is_rebuilt_quickly_despite_long_interval() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let a = fake(vec![]);
        let mut cfg = config(&dir.path().join("n.db"), vec![a]);
        cfg.snapshot_interval = Duration::from_secs(30);
        let token = CancellationToken::new();
        let (mut rx, handle) = spawn(cfg, token.clone()).unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            wait_until(&mut rx, |s| s.nodes.iter().any(|n| n.addr == a)),
        )
        .await
        .expect("snapshot still empty after 5 s");
        token.cancel();
        handle.await.unwrap().unwrap();
    })
    .await;
}

#[test]
fn crawler_config_takes_addr_wait_from_the_preset() {
    let mut preset = builtin("bitcoin-mainnet").unwrap();
    preset.addr_wait = Duration::from_secs(45);
    let cfg = CrawlerConfig::new(preset, "unused.db".into());
    assert_eq!(cfg.probe.addr_wait, Duration::from_secs(45));
}

/// Collects formatted log output, as `src/cloudflare/publish.rs` tests do.
#[derive(Clone, Default)]
struct Logs(Arc<std::sync::Mutex<Vec<u8>>>);

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

impl Logs {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

/// Captures this thread's log output until the guard drops. With a current-thread runtime every
/// crawler task runs on this thread, under the scoped subscriber.
fn capture_logs() -> (Logs, tracing::subscriber::DefaultGuard) {
    let logs = Logs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (logs, guard)
}

/// A `headers` payload with one header building on `prev`.
fn one_header(prev: [u8; 32]) -> Vec<u8> {
    let mut reply = vec![1u8];
    reply.extend(4i32.to_le_bytes());
    reply.extend(prev);
    reply.extend([0u8; 45]);
    reply
}

const CHECKPOINT: Checkpoint = Checkpoint {
    height: 50,
    hash: [0x11; 32],
};

fn chain_of(path: &Path, addr: SocketAddr) -> Option<i64> {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row(
        "SELECT chain FROM nodes WHERE ip = ?1 AND port = ?2",
        rusqlite::params![addr.ip().to_string(), addr.port()],
        |r| r.get(0),
    )
    .ok()
}

#[tokio::test(flavor = "current_thread")]
async fn wrong_checkpoint_warns_and_serves_nothing() {
    let (logs, _guard) = capture_logs();
    let checkpoint = CHECKPOINT;
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("n.db");
        let p = preset();
        let reply = one_header([0x22; 32]);
        let peers: Vec<SocketAddr> = (0..3)
            .map(|_| {
                let mut s = FakeScript::new(p.magic, p.protocol_version);
                s.headers_reply = Some(reply.clone());
                FakePeer::spawn(s).0
            })
            .collect();
        let mut cfg = config(&path, peers.clone());
        cfg.preset.checkpoint = Some(checkpoint.clone());
        let token = CancellationToken::new();
        let (rx, handle) = spawn(cfg, token.clone()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !logs.text().contains("wrong chain") {
            assert!(Instant::now() < deadline, "no warning: {}", logs.text());
            assert!(rx.borrow().nodes.is_empty(), "a wrong-chain node is served");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        token.cancel();
        handle.await.unwrap().unwrap();
        assert!(rx.borrow().nodes.is_empty());
        let conn = rusqlite::Connection::open(&path).unwrap();
        let wrong: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE chain = 2 AND last_ok = 0",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(wrong, 3);
    })
    .await;
    let text = logs.text();
    let line = text
        .lines()
        .find(|l| l.contains("wrong chain"))
        .unwrap()
        .to_string();
    assert!(line.contains("WARN"), "{line}");
    assert!(line.contains(&checkpoint.display_hash()), "{line}");
    assert!(line.contains("50"), "{line}");
}

#[tokio::test(flavor = "current_thread")]
async fn one_fork_node_among_verified_nodes_does_not_warn() {
    let (logs, _guard) = capture_logs();
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("n.db");
        let p = preset();
        let reference = CHECKPOINT.display_hash();
        let now = unix_now();
        // Three nodes verified under this checkpoint, due for a routine (non-checking) re-probe.
        let verified: Vec<SocketAddr> = (0..3)
            .map(|_| {
                let mut s = FakeScript::new(p.magic, p.protocol_version);
                s.headers_reply = Some(one_header(CHECKPOINT.hash));
                FakePeer::spawn(s).0
            })
            .collect();
        {
            let store = NodeStore::open(&path).unwrap();
            for a in &verified {
                store
                    .record(
                        *a,
                        Outcome::Good {
                            version: p.protocol_version,
                            services: 1,
                            height: 100,
                            harvested: true,
                            chain: ChainCheck::Verified,
                        },
                        now - 900,
                        Some(&reference),
                    )
                    .unwrap();
            }
        }
        // One fork node, learned at bootstrap.
        let mut s = FakeScript::new(p.magic, p.protocol_version);
        s.headers_reply = Some(one_header([0x22; 32]));
        let fork = FakePeer::spawn(s).0;
        let mut cfg = config(&path, vec![fork]);
        cfg.preset.checkpoint = Some(CHECKPOINT);
        // One interval covers every probe below, with room for a busy machine.
        cfg.snapshot_interval = Duration::from_secs(8);
        let started = Instant::now();
        let token = CancellationToken::new();
        let (rx, handle) = spawn(cfg, token.clone()).unwrap();
        assert_eq!(rx.borrow().nodes.len(), 3);
        let deadline = started + Duration::from_secs(6);
        while chain_of(&path, fork) != Some(2) {
            assert!(
                Instant::now() < deadline,
                "fork node not classified within 6 s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // Let the interval close and its tally be judged.
        tokio::time::sleep(
            (started + Duration::from_millis(8500)).saturating_duration_since(Instant::now()),
        )
        .await;
        token.cancel();
        handle.await.unwrap().unwrap();
        for a in &verified {
            assert_eq!(chain_of(&path, *a), Some(1));
        }
    })
    .await;
    let text = logs.text();
    assert!(!text.contains("wrong chain"), "{text}");
}

#[tokio::test(flavor = "current_thread")]
async fn wrong_chain_gossip_is_dropped_and_its_harvest_recorded() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("n.db");
        let p = preset();
        let mut s = FakeScript::new(p.magic, p.protocol_version);
        s.headers_reply = Some(one_header([0x22; 32]));
        s.addr_reply = vec![
            entry("127.0.0.1:1".parse().unwrap()),
            entry("127.0.0.1:2".parse().unwrap()),
        ];
        let fork = FakePeer::spawn(s).0;
        let mut cfg = config(&path, vec![fork]);
        cfg.preset.checkpoint = Some(CHECKPOINT);
        let token = CancellationToken::new();
        let (_rx, handle) = spawn(cfg, token.clone()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while chain_of(&path, fork) != Some(2) {
            assert!(Instant::now() < deadline, "fork node not classified");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        token.cancel();
        handle.await.unwrap().unwrap();
        assert_eq!(
            row_count(&path),
            1,
            "gossip from a wrong-chain peer was stored"
        );
        let store = NodeStore::open(&path).unwrap();
        let reference = CHECKPOINT.display_hash();
        let due = store
            .due(unix_now() + 700, 10, true, Some(&reference))
            .unwrap();
        let next = due
            .iter()
            .find(|d| d.addr == fork)
            .expect("fork node not due");
        assert!(!next.harvest && next.check_chain, "{next:?}");
    })
    .await;
}
