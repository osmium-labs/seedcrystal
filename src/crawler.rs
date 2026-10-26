//! Background crawler: probes known nodes, learns more from their `addr`
//! gossip, and publishes the servable set as an immutable snapshot.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use tokio::sync::{Semaphore, watch};
use tokio::task::{Id, JoinHandle, JoinSet};
use tokio::time::{MissedTickBehavior, interval, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::coin::{Checkpoint, CoinPreset};
use crate::p2p::{AddrEntry, ChainCheck, NetAddr, ProbeOptions, probe};
use crate::store::{Due, NodeStore, Outcome, Servable, ServablePolicy};

/// Learned entries accepted from one peer response.
const MAX_LEARNED_PER_RESPONSE: usize = 1000;
const SCHEDULER_TICK: Duration = Duration::from_secs(1);
const DNS_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// What the DNS side serves: an immutable view of the store at `built_at`.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub nodes: Vec<Servable>,
    pub built_at: i64,
}

/// Receiver of the latest published snapshot.
pub type SnapshotRx = watch::Receiver<Arc<Snapshot>>;

/// Extra bootstrap addresses, asked again on every bootstrap attempt.
pub type BootstrapSource = Arc<dyn Fn() -> Vec<SocketAddr> + Send + Sync>;

#[derive(Clone)]
pub struct CrawlerConfig {
    pub preset: CoinPreset,
    pub store_path: PathBuf,
    /// Extra bootstrap addresses, in addition to the preset's.
    pub bootstrap: Vec<SocketAddr>,
    /// Maximum simultaneous probes.
    pub concurrency: usize,
    pub snapshot_interval: Duration,
    /// Probe and serve loopback/private addresses on any port. For tests and
    /// private networks only; off, only routable peers on the preset's
    /// default port are probed and served.
    pub allow_private: bool,
    pub probe: ProbeOptions,
    /// While the seeder has nothing to serve, bootstrap is repeated this often.
    pub bootstrap_retry: Duration,
    /// Optional dynamic bootstrap source, consulted on every attempt.
    pub bootstrap_source: Option<BootstrapSource>,
}

impl CrawlerConfig {
    /// Defaults: 64 concurrent probes, a snapshot every 30 s, public peers only.
    pub fn new(preset: CoinPreset, store_path: PathBuf) -> Self {
        let probe = ProbeOptions {
            addr_wait: preset.addr_wait,
            ..ProbeOptions::default()
        };
        Self {
            preset,
            store_path,
            bootstrap: Vec::new(),
            concurrency: 64,
            snapshot_interval: Duration::from_secs(30),
            allow_private: false,
            probe,
            bootstrap_retry: Duration::from_secs(60),
            bootstrap_source: None,
        }
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

fn entry_for(addr: SocketAddr, now: i64) -> AddrEntry {
    AddrEntry {
        addr: match addr.ip() {
            std::net::IpAddr::V4(a) => NetAddr::V4(a),
            std::net::IpAddr::V6(a) => NetAddr::V6(a),
        },
        port: addr.port(),
        services: 0,
        time: now.clamp(0, i64::from(u32::MAX)) as u32,
    }
}

/// Run a synchronous store call off the async threads.
async fn blocking<T, F>(store: &Arc<NodeStore>, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&NodeStore) -> Result<T> + Send + 'static,
{
    let store = Arc::clone(store);
    tokio::task::spawn_blocking(move || f(&store))
        .await
        .context("store task panicked")?
}

struct Shared {
    store: Arc<NodeStore>,
    policy: ServablePolicy,
    accept_port: Option<u16>,
    preset_id: String,
    checkpoint: Option<Checkpoint>,
    /// Checkpoint verdicts since the last snapshot.
    tally: Mutex<ChainTally>,
    /// When the wrong-checkpoint warning last fired.
    last_warning: Mutex<Option<std::time::Instant>>,
}

#[derive(Default)]
struct ChainTally {
    verified: u32,
    wrong: u32,
}

impl Shared {
    fn count(&self, chain: ChainCheck) {
        let mut t = self.tally.lock().unwrap_or_else(|e| e.into_inner());
        match chain {
            ChainCheck::Verified => t.verified += 1,
            ChainCheck::WrongChain => t.wrong += 1,
            ChainCheck::NotChecked | ChainCheck::Unverified => {}
        }
    }

    /// A mistyped checkpoint makes every node wrong chain and empties the seed, so say so
    /// when wrong chain is the majority of this interval's verdicts, at most once per
    /// [`WRONG_CHECKPOINT_WARN_EVERY`]: an empty seed rebuilds every 2 s. Returns whether it
    /// warned.
    fn warn_on_wrong_checkpoint(&self) -> bool {
        let t = std::mem::take(&mut *self.tally.lock().unwrap_or_else(|e| e.into_inner()));
        let Some(c) = &self.checkpoint else {
            return false;
        };
        if t.wrong >= 1 && t.wrong > t.verified {
            let mut last = self.last_warning.lock().unwrap_or_else(|e| e.into_inner());
            if last.is_some_and(|at| at.elapsed() < WRONG_CHECKPOINT_WARN_EVERY) {
                return false;
            }
            *last = Some(std::time::Instant::now());
            warn!(
                checkpoint_height = c.height,
                checkpoint_hash = %c.display_hash(),
                "{}: {} of {} checked nodes are on the wrong chain; is the preset checkpoint right?",
                self.preset_id,
                t.wrong,
                t.wrong + t.verified
            );
            return true;
        }
        false
    }
}

async fn build_snapshot(shared: &Shared) -> Result<Snapshot> {
    let policy = shared.policy.clone();
    let now = unix_now();
    let nodes = blocking(&shared.store, move |s| s.servable(&policy, now)).await?;
    Ok(Snapshot {
        nodes,
        built_at: now,
    })
}

/// The servable policy for `preset`; the crawler and the `seeds` command
/// both build theirs here so they cannot disagree.
pub fn policy_for(preset: &CoinPreset, allow_private: bool) -> ServablePolicy {
    ServablePolicy {
        port: if allow_private {
            None
        } else {
            Some(preset.default_port)
        },
        serve_floor_base: preset.min_serve_version,
        schedule: preset.serve_version_schedule.clone(),
        min_height: preset.min_height,
        tip_tolerance: preset.tip_tolerance,
        warmup_floor: 10,
        allow_private,
        checkpoint_ref: preset.checkpoint.as_ref().map(|c| c.display_hash()),
    }
}

/// Starts the crawler. The first snapshot is built from the store before any
/// probing, so the receiver is immediately populated on a restart. Must be
/// called inside a tokio runtime. Cancelling `shutdown` stops all network work;
/// the returned handle resolves only once no store call is still running.
pub fn spawn(
    cfg: CrawlerConfig,
    shutdown: CancellationToken,
) -> Result<(SnapshotRx, JoinHandle<Result<()>>)> {
    let store = Arc::new(NodeStore::open(&cfg.store_path)?);
    let policy = policy_for(&cfg.preset, cfg.allow_private);
    let now = unix_now();
    let first = Snapshot {
        nodes: store.servable(&policy, now)?,
        built_at: now,
    };
    let (tx, rx) = watch::channel(Arc::new(first));
    let shared = Arc::new(Shared {
        store,
        accept_port: policy.port,
        policy,
        preset_id: cfg.preset.id.clone(),
        checkpoint: cfg.preset.checkpoint.clone(),
        tally: Mutex::default(),
        last_warning: Mutex::default(),
    });
    let handle = tokio::spawn(run(cfg, shared, tx, shutdown));
    Ok((rx, handle))
}

/// Rebuild period while the published snapshot is empty.
const EMPTY_SNAPSHOT_PERIOD: Duration = Duration::from_secs(2);

/// Least time between two wrong-checkpoint warnings.
const WRONG_CHECKPOINT_WARN_EVERY: Duration = Duration::from_secs(10 * 60);

/// Longest we wait for tasks to finish their store writes after shutdown.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Wait for every task to finish on its own; abort stragglers after the grace period.
async fn drain<T: 'static>(set: &mut JoinSet<T>) {
    let all = async { while set.join_next().await.is_some() {} };
    if timeout(SHUTDOWN_GRACE, all).await.is_err() {
        warn!("tasks did not finish after shutdown; aborting");
        set.abort_all();
        while set.join_next().await.is_some() {}
    }
}

async fn run(
    cfg: CrawlerConfig,
    shared: Arc<Shared>,
    tx: watch::Sender<Arc<Snapshot>>,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut aux: JoinSet<()> = JoinSet::new();
    aux.spawn(bootstrap_task(
        cfg.clone(),
        Arc::clone(&shared),
        tx.subscribe(),
        shutdown.clone(),
    ));
    aux.spawn(snapshot_task(
        cfg.snapshot_interval,
        Arc::clone(&shared),
        tx,
        shutdown.clone(),
    ));

    let result = schedule(&cfg, &shared, &shutdown).await;
    drain(&mut aux).await;
    result
}

async fn bootstrap_attempt(cfg: &CrawlerConfig, shared: &Shared, shutdown: &CancellationToken) {
    let port = cfg.preset.default_port;
    let mut addrs: Vec<SocketAddr> = cfg.preset.bootstrap_fixed.clone();
    addrs.extend(cfg.bootstrap.iter().copied());
    if let Some(source) = &cfg.bootstrap_source {
        addrs.extend(source());
    }
    for name in &cfg.preset.bootstrap_dns {
        let lookup = timeout(
            DNS_LOOKUP_TIMEOUT,
            tokio::net::lookup_host((name.as_str(), port)),
        );
        let result = tokio::select! {
            () = shutdown.cancelled() => return,
            r = lookup => r,
        };
        match result {
            Ok(Ok(found)) => addrs.extend(found),
            Ok(Err(e)) => warn!(%name, error = %e, "bootstrap lookup failed"),
            Err(_) => warn!(%name, "bootstrap lookup timed out"),
        }
    }
    let now = unix_now();
    let entries: Vec<AddrEntry> = addrs.into_iter().map(|a| entry_for(a, now)).collect();
    let accept = shared.accept_port;
    match blocking(&shared.store, move |s| s.add_learned(&entries, now, accept)).await {
        Ok(n) => debug!(added = n, "bootstrap addresses stored"),
        Err(e) => warn!(error = %e, "storing bootstrap addresses failed"),
    }
}

/// Seeds the store, then re-seeds every `bootstrap_retry` while there is
/// nothing to serve, so a transient failure on a cold start heals itself.
async fn bootstrap_task(
    cfg: CrawlerConfig,
    shared: Arc<Shared>,
    snapshots: watch::Receiver<Arc<Snapshot>>,
    shutdown: CancellationToken,
) {
    let mut tick = interval(cfg.bootstrap_retry.max(Duration::from_millis(10)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick.tick().await;
    bootstrap_attempt(&cfg, &shared, &shutdown).await;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        if snapshots.borrow().nodes.is_empty() {
            bootstrap_attempt(&cfg, &shared, &shutdown).await;
        }
    }
}

async fn snapshot_task(
    period: Duration,
    shared: Arc<Shared>,
    tx: watch::Sender<Arc<Snapshot>>,
    shutdown: CancellationToken,
) {
    let period = period.max(Duration::from_millis(10));
    loop {
        // A seeder with nothing to serve rebuilds quickly, so its first nodes appear soon.
        let wait = if tx.borrow().nodes.is_empty() {
            period.min(EMPTY_SNAPSHOT_PERIOD)
        } else {
            period
        };
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = sleep(wait) => {}
        }
        match build_snapshot(&shared).await {
            Ok(s) => {
                let was = tx.borrow().nodes.len();
                if s.nodes.len() != was {
                    info!(
                        "servable nodes for {}: {} (was {was})",
                        shared.preset_id,
                        s.nodes.len()
                    );
                }
                tx.send_replace(Arc::new(s));
            }
            Err(e) => warn!(error = %e, "snapshot rebuild failed"),
        }
        let _ = shared.warn_on_wrong_checkpoint();
    }
}

async fn schedule(
    cfg: &CrawlerConfig,
    shared: &Arc<Shared>,
    shutdown: &CancellationToken,
) -> Result<()> {
    let concurrency = cfg.concurrency.max(1);
    let permits = Arc::new(Semaphore::new(concurrency));
    let mut probes: JoinSet<SocketAddr> = JoinSet::new();
    // Keyed by task id so a panicked probe is released too (a JoinError has no address).
    let mut in_flight: HashMap<Id, SocketAddr> = HashMap::new();
    let mut tick = interval(SCHEDULER_TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            Some(done) = probes.join_next_with_id(), if !probes.is_empty() => {
                let id = match done {
                    Ok((id, _)) => id,
                    Err(e) => {
                        warn!(error = %e, "probe task failed");
                        e.id()
                    }
                };
                in_flight.remove(&id);
            }
            _ = tick.tick() => {
                let free = permits.available_permits();
                if free == 0 {
                    continue;
                }
                let limit = concurrency * 2 + in_flight.len();
                let allow_private = cfg.allow_private;
                let now = unix_now();
                let checkpoint_ref = shared.policy.checkpoint_ref.clone();
                let due = match blocking(&shared.store, move |s| {
                    s.due(now, limit, allow_private, checkpoint_ref.as_deref())
                })
                .await
                {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(error = %e, "reading due nodes failed");
                        continue;
                    }
                };
                for Due { addr, harvest, check_chain } in due {
                    if in_flight.values().any(|a| *a == addr) {
                        continue;
                    }
                    let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                        break;
                    };
                    let shared = Arc::clone(shared);
                    let preset = cfg.preset.clone();
                    let opts = ProbeOptions {
                        want_addrs: harvest,
                        check_chain,
                        ..cfg.probe.clone()
                    };
                    let token = shutdown.clone();
                    let handle = probes.spawn(async move {
                        probe_one(addr, harvest, &preset, &opts, &shared, &token).await;
                        drop(permit);
                        addr
                    });
                    in_flight.insert(handle.id(), addr);
                }
            }
        }
    }
    // Probes notice the token themselves; each finishes its store write first.
    drain(&mut probes).await;
    Ok(())
}

async fn probe_one(
    addr: SocketAddr,
    harvest: bool,
    preset: &CoinPreset,
    opts: &ProbeOptions,
    shared: &Shared,
    shutdown: &CancellationToken,
) {
    let result = tokio::select! {
        () = shutdown.cancelled() => return,
        r = probe(addr, preset, opts) => r,
    };
    let now = unix_now();
    if let Ok(r) = &result {
        // A node not due for a check is already verified under this checkpoint; a good
        // handshake keeps it so, which balances the tally against fork nodes in back-off.
        if !opts.check_chain && shared.checkpoint.is_some() {
            shared.count(ChainCheck::Verified);
        } else {
            shared.count(r.chain);
        }
    }
    let (outcome, mut learned) = match result {
        // Its gossip is likely of the other network, so it is not learned.
        Ok(r) if r.chain == ChainCheck::WrongChain => {
            (Outcome::WrongChain { harvested: harvest }, Vec::new())
        }
        Ok(r) => (
            Outcome::Good {
                version: r.peer.version,
                services: r.peer.services,
                height: r.peer.start_height,
                harvested: harvest,
                chain: r.chain,
            },
            r.addrs,
        ),
        Err(e) => {
            debug!(%addr, error = %e, "probe failed");
            (Outcome::Bad, Vec::new())
        }
    };
    learned.truncate(MAX_LEARNED_PER_RESPONSE);
    let accept = shared.accept_port;
    let checkpoint_ref = shared.policy.checkpoint_ref.clone();
    let stored = blocking(&shared.store, move |s| {
        s.record(addr, outcome, now, checkpoint_ref.as_deref())?;
        s.add_learned(&learned, now, accept)
    })
    .await;
    if let Err(e) = stored {
        warn!(%addr, error = %e, "recording probe outcome failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared(dir: &std::path::Path, checkpoint: Option<Checkpoint>) -> Shared {
        let preset = crate::coin::builtin("bitcoin-mainnet").unwrap();
        Shared {
            store: Arc::new(NodeStore::open(&dir.join("n.db")).unwrap()),
            policy: policy_for(&preset, false),
            accept_port: None,
            preset_id: preset.id,
            checkpoint,
            tally: Mutex::default(),
            last_warning: Mutex::default(),
        }
    }

    const CP: Checkpoint = Checkpoint {
        height: 100,
        hash: [0x11; 32],
    };

    #[test]
    fn wrong_checkpoint_warning_fires_once_per_period() {
        let dir = tempfile::tempdir().unwrap();
        let s = shared(dir.path(), Some(CP));
        for _ in 0..3 {
            s.count(ChainCheck::WrongChain);
        }
        assert!(s.warn_on_wrong_checkpoint());
        // The next intervals are just as wrong, but the warning waits out its period.
        for _ in 0..5 {
            s.count(ChainCheck::WrongChain);
            assert!(!s.warn_on_wrong_checkpoint());
        }
        // Once the period has passed it fires again.
        *s.last_warning.lock().unwrap() =
            Some(std::time::Instant::now() - WRONG_CHECKPOINT_WARN_EVERY);
        s.count(ChainCheck::WrongChain);
        assert!(s.warn_on_wrong_checkpoint());
    }

    #[test]
    fn no_warning_on_a_verified_majority_or_without_a_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let s = shared(dir.path(), Some(CP));
        s.count(ChainCheck::WrongChain);
        s.count(ChainCheck::Verified);
        s.count(ChainCheck::Verified);
        assert!(!s.warn_on_wrong_checkpoint());
        let none = shared(dir.path(), None);
        none.count(ChainCheck::WrongChain);
        assert!(!none.warn_on_wrong_checkpoint());
    }
}
