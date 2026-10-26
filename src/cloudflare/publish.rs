//! Keeps Cloudflare's A and AAAA records for a seed in step with the crawler's snapshot.
//!
//! The publisher touches only records whose comment is exactly [`OWNER_COMMENT`]. It never
//! writes from an empty snapshot, and it changes as little as it can: records that are still
//! servable stay, only the gap is filled.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use hickory_proto::rr::Name;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::cloudflare::api::{
    ApiError, Batch, Client, NewRecord, OWNER_COMMENT, Record, RecordKind, normalize_name,
};
use crate::crawler::{Snapshot, SnapshotRx};
use crate::dns::answer::{Family, MAX_PER_GROUP, matching, netgroup, published_names, select_with};

/// Cloudflare's batch limit on the Free plan.
const MAX_BATCH_OPS: usize = 200;
const MAX_BACKOFF: Duration = Duration::from_secs(30 * 60);
const MIN_TTL: u32 = 60;
/// Keeps one name's deletes and posts (at most four times this, over both families) inside a
/// single batch.
pub const MAX_PER_NAME: usize = 50;
/// How long the servable-count high-water mark is remembered.
const SHRINK_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);
/// Samples further in the future than this are dropped as clock jumps.
const FUTURE_SLACK_SECS: u64 = 60;
/// Samples are kept one per minute, so the state file holds at most a day of minutes.
const BUCKET_SECS: u64 = 60;
const MAX_SAMPLES: usize = (SHRINK_WINDOW.as_secs() / BUCKET_SECS) as usize;
/// Records replaced per name and family in one rotation step.
const ROTATE_PER_CYCLE: usize = 2;

#[derive(Clone, Debug)]
pub struct PublishConfig {
    pub apex: Name,
    pub filters: Vec<u64>,
    pub seed_flags: u64,
    /// At least 60 seconds.
    pub ttl: u32,
    pub interval: Duration,
    /// Addresses published per name and family.
    pub per_name: usize,
    pub dry_run: bool,
    /// Where the shrink guard's samples survive a restart; `None` keeps them in memory only.
    pub state_path: Option<PathBuf>,
    /// How long the startup check waits before retrying a `Zone` error once.
    pub startup_retry: Duration,
}

impl PublishConfig {
    /// The defaults: TTL 60 s, a cycle every 5 minutes, 20 addresses per name.
    pub fn new(apex: Name, filters: Vec<u64>, seed_flags: u64) -> Self {
        Self {
            apex,
            filters,
            seed_flags,
            ttl: MIN_TTL,
            interval: Duration::from_secs(300),
            per_name: 20,
            dry_run: false,
            state_path: None,
            startup_retry: Duration::from_secs(5),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.ttl < MIN_TTL {
            bail!(
                "Cloudflare TTL must be at least {MIN_TTL} seconds (got {})",
                self.ttl
            );
        }
        if !(1..=MAX_PER_NAME).contains(&self.per_name) {
            bail!("per_name must be between 1 and {MAX_PER_NAME}");
        }
        if self.interval.is_zero() {
            bail!("the publish interval must be greater than zero");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CycleReport {
    pub created: usize,
    pub deleted: usize,
    pub kept: usize,
    /// Names that carry A or AAAA records we do not own.
    pub foreign: Vec<String>,
    pub skipped_empty: bool,
    /// The snapshot is under half of the 24 h high-water mark: the cycle only filled gaps.
    pub shrink_guard: bool,
}

/// What the publisher remembers between cycles: the servable counts of the last 24 hours, as
/// `(unix seconds, count)`.
#[derive(Debug, Default)]
pub struct CycleState {
    samples: VecDeque<(u64, usize)>,
    dirty: bool,
}

#[derive(Serialize, Deserialize)]
struct StateFile {
    samples: Vec<(u64, usize)>,
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl CycleState {
    /// Loads the samples saved at `path`, dropping the stale and the future ones. A missing
    /// file starts empty silently; an unreadable or corrupt one starts empty with a warning.
    pub fn load(path: &Path) -> Self {
        Self::load_at(path, unix_now())
    }

    fn load_at(path: &Path, now: u64) -> Self {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Self::default(),
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e,
                    "could not read the shrink-guard state; starting empty");
                return Self::default();
            }
        };
        match serde_json::from_str::<StateFile>(&text) {
            Ok(f) => {
                let total = f.samples.len();
                let future = f
                    .samples
                    .iter()
                    .filter(|&&(at, _)| at > now + FUTURE_SLACK_SECS)
                    .count();
                if future > 0 {
                    tracing::warn!(path = %path.display(), dropped = future,
                        "the shrink-guard state has samples from the future; ignoring them");
                }
                let mut kept: Vec<(u64, usize)> = f
                    .samples
                    .into_iter()
                    .filter(|&(at, _)| Self::in_window(now, at))
                    .collect();
                kept.sort_by_key(|&(at, _)| at);
                let mut state = Self::default();
                for (at, count) in kept {
                    state.add(at, count);
                }
                tracing::debug!(path = %path.display(), file = total, kept = state.samples.len(),
                    "loaded the shrink-guard state");
                state.dirty = false;
                state
            }
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e,
                    "the shrink-guard state is corrupt; starting empty");
                Self::default()
            }
        }
    }

    /// Adds a sample, folding it into the last one when both fall in the same minute (the
    /// larger count wins).
    fn add(&mut self, at: u64, count: usize) {
        match self.samples.back_mut() {
            Some(last) if last.0 / BUCKET_SECS == at / BUCKET_SECS => last.1 = last.1.max(count),
            _ => self.samples.push_back((at, count)),
        }
    }

    fn in_window(now: u64, at: u64) -> bool {
        at <= now + FUTURE_SLACK_SECS && now.saturating_sub(at) <= SHRINK_WINDOW.as_secs()
    }

    /// Writes the samples to `path` through a synced temp file in the same directory and a
    /// rename.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let skip = self.samples.len().saturating_sub(MAX_SAMPLES);
        let body = serde_json::to_vec(&StateFile {
            samples: self.samples.iter().skip(skip).copied().collect(),
        })
        .map_err(std::io::Error::other)?;
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        let write = || -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&body)?;
            f.sync_all()?;
            std::fs::rename(&tmp, path)
        };
        let result = write();
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        result
    }

    /// Records this cycle's servable count and says whether it fell below half of the highest
    /// count seen in the last 24 hours (this sample included).
    fn observe(&mut self, now: u64, count: usize) -> Option<usize> {
        self.samples.retain(|&(at, _)| Self::in_window(now, at));
        let mark = self.samples.iter().map(|&(_, c)| c).max().unwrap_or(0);
        self.add(now, count);
        self.dirty = true;
        (count * 2 < mark).then_some(mark)
    }

    fn persist(&mut self, path: &Path) {
        if !self.dirty {
            return;
        }
        match self.save(path) {
            Ok(()) => self.dirty = false,
            Err(e) => tracing::warn!(path = %path.display(), error = %e,
                "could not save the shrink-guard state; continuing without it"),
        }
    }
}

/// The planned changes for one name.
struct NamePlan {
    name: String,
    deletes: Vec<String>,
    posts: Vec<NewRecord>,
    would_delete: Vec<(RecordKind, IpAddr)>,
}

impl NamePlan {
    fn ops(&self) -> usize {
        self.deletes.len() + self.posts.len()
    }
}

fn owned(r: &Record) -> bool {
    r.comment.as_deref() == Some(OWNER_COMMENT)
}

/// One pass with no memory of earlier cycles (so no shrink guard) and no shutdown.
pub async fn reconcile_once(
    client: &Client,
    cfg: &PublishConfig,
    snapshot: &Snapshot,
    rng: &mut impl Rng,
) -> Result<CycleReport, ApiError> {
    reconcile(
        client,
        cfg,
        snapshot,
        rng,
        &mut CycleState::default(),
        &CancellationToken::new(),
    )
    .await
}

/// One pass: compare Cloudflare with `snapshot` and bring it in line. Stops quietly, before the
/// next API call, once `shutdown` is cancelled; every batch is atomic, so that is safe.
pub async fn reconcile(
    client: &Client,
    cfg: &PublishConfig,
    snapshot: &Snapshot,
    rng: &mut impl Rng,
    state: &mut CycleState,
    shutdown: &CancellationToken,
) -> Result<CycleReport, ApiError> {
    let mut report = CycleReport::default();
    if snapshot.nodes.is_empty() {
        tracing::warn!(apex = %cfg.apex, "snapshot is empty; leaving Cloudflare records as they are");
        report.skipped_empty = true;
        return Ok(report);
    }

    let guard = state.observe(unix_now(), snapshot.nodes.len());
    if let Some(mark) = guard {
        tracing::warn!(
            apex = %cfg.apex, servable = snapshot.nodes.len(), high_water_mark = mark,
            "the servable node count fell below half of its 24 h high-water mark; \
             filling gaps but deleting nothing and not rotating"
        );
        report.shrink_guard = true;
    }
    let has_v4 = snapshot.nodes.iter().any(|n| n.addr.is_ipv4());
    let has_v6 = snapshot.nodes.iter().any(|n| n.addr.is_ipv6());
    let mut plans = Vec::new();
    for (name, target) in published_names(&cfg.apex, &cfg.filters) {
        if shutdown.is_cancelled() {
            return Ok(report);
        }
        let name = normalize_name(&name.to_ascii());
        let existing = client.records_for(&name).await?;
        let foreign = existing.iter().filter(|r| !owned(r)).count();
        if foreign > 0 {
            tracing::warn!(
                name = %name, count = foreign,
                "records on this name are not managed by seedcrystal; leaving them alone"
            );
            report.foreign.push(name.clone());
        }
        let mut plan = NamePlan {
            name: name.clone(),
            deletes: Vec::new(),
            posts: Vec::new(),
            would_delete: Vec::new(),
        };
        for (family, kind) in [(Family::V4, RecordKind::A), (Family::V6, RecordKind::Aaaa)] {
            // No node of this family anywhere: the crawler probably lost that routing, so
            // leave the family's records alone this cycle.
            if !(if family == Family::V4 { has_v4 } else { has_v6 }) {
                continue;
            }
            let servable_set: HashSet<IpAddr> =
                matching(&snapshot.nodes, &target, cfg.seed_flags, family)
                    .into_iter()
                    .collect();
            // Cloudflare rejects an identical record, so never post an address a foreign
            // record already holds on this name. Foreign records do not count toward the
            // group limit.
            let foreign_ips: HashSet<IpAddr> = existing
                .iter()
                .filter(|r| r.kind == kind && !owned(r))
                .map(|r| r.content)
                .collect();
            let mut kept: HashSet<IpAddr> = HashSet::new();
            let mut kept_ips: Vec<IpAddr> = Vec::new();
            let mut per_group: HashMap<[u8; 4], usize> = HashMap::new();
            let mut kept_records: Vec<&Record> = Vec::new();
            let (deletes_before, posts_before) = (plan.deletes.len(), plan.posts.len());
            // Sorted by address, then id, so which over-limit record survives does not depend
            // on the order Cloudflare lists them in.
            let mut owned_records: Vec<&Record> = existing
                .iter()
                .filter(|r| r.kind == kind && owned(r))
                .collect();
            owned_records.sort_by(|a, b| (a.content, &a.id).cmp(&(b.content, &b.id)));
            for r in owned_records {
                // A record with the wrong TTL or behind Cloudflare's proxy is not what we
                // publish; replace it. One past its group's limit goes too, the lowest
                // address staying.
                let as_published = r.ttl == cfg.ttl && !r.proxied;
                let group = per_group.entry(netgroup(&r.content)).or_default();
                if as_published
                    && servable_set.contains(&r.content)
                    && kept.len() < cfg.per_name
                    && *group < MAX_PER_GROUP
                    && kept.insert(r.content)
                {
                    *group += 1;
                    kept_ips.push(r.content);
                    report.kept += 1;
                    kept_records.push(r);
                } else if guard.is_none() {
                    plan.deletes.push(r.id.clone());
                    plan.would_delete.push((kind, r.content));
                } else {
                    // Shrink guard: stale records stay. They still occupy a slot.
                    if kept.insert(r.content) {
                        *group += 1;
                        kept_ips.push(r.content);
                    }
                    report.kept += 1;
                }
            }
            let mut skip = foreign_ips.clone();
            skip.extend(kept.iter().copied());
            let fill = select_with(
                &snapshot.nodes,
                &target,
                cfg.seed_flags,
                family,
                cfg.per_name,
                &kept_ips,
                &skip,
                rng,
            );
            for ip in fill {
                plan.posts.push(NewRecord {
                    name: name.clone(),
                    kind,
                    content: ip,
                    ttl: cfg.ttl,
                });
            }
            // Nothing else changed: swap a few records for addresses not yet published, so
            // every address is eventually published. A swapped-in address may not push its
            // group past the limit.
            let unchanged =
                plan.deletes.len() == deletes_before && plan.posts.len() == posts_before;
            // A name the group limit leaves short has every group with candidates saturated, so a
            // swap would only churn.
            let full = kept_ips.len() >= cfg.per_name;
            if guard.is_none() && unchanged && full && !kept_records.is_empty() {
                let n = ROTATE_PER_CYCLE.min(kept_records.len());
                let out: Vec<&Record> = kept_records.sample(rng, n).copied().collect();
                let out_ips: HashSet<IpAddr> = out.iter().map(|r| r.content).collect();
                let remaining: Vec<IpAddr> = kept_ips
                    .iter()
                    .copied()
                    .filter(|ip| !out_ips.contains(ip))
                    .collect();
                let swapped_in = select_with(
                    &snapshot.nodes,
                    &target,
                    cfg.seed_flags,
                    family,
                    remaining.len() + n,
                    &remaining,
                    &skip,
                    rng,
                );
                for (r, ip) in out.into_iter().zip(swapped_in) {
                    plan.deletes.push(r.id.clone());
                    plan.would_delete.push((kind, r.content));
                    plan.posts.push(NewRecord {
                        name: name.clone(),
                        kind,
                        content: ip,
                        ttl: cfg.ttl,
                    });
                    report.kept -= 1;
                }
            }
        }
        report.deleted += plan.deletes.len();
        report.created += plan.posts.len();
        plans.push(plan);
    }

    let apex = normalize_name(&cfg.apex.to_ascii());
    if cfg.dry_run {
        tracing::info!(
            apex = %apex, dry_run = true,
            would_create = report.created, would_delete = report.deleted, kept = report.kept,
            "reconciled (dry run)"
        );
        for p in &plans {
            for (kind, ip) in &p.would_delete {
                tracing::info!("would delete {} {kind:?} {ip}", p.name);
            }
            for post in &p.posts {
                tracing::info!("would create {} {:?} {}", p.name, post.kind, post.content);
            }
        }
        return Ok(report);
    }
    tracing::info!(
        apex = %apex, dry_run = false,
        created = report.created, deleted = report.deleted, kept = report.kept,
        "reconciled"
    );

    for batch in chunk(plans) {
        if shutdown.is_cancelled() {
            return Ok(report);
        }
        client.apply(&batch).await?;
    }
    Ok(report)
}

/// Packs the plans into batches of at most [`MAX_BATCH_OPS`] operations. A name's operations
/// stay together unless the name alone is too big. Delete-heavy plans go first.
fn chunk(mut plans: Vec<NamePlan>) -> Vec<Batch> {
    plans.retain(|p| p.ops() > 0);
    plans.sort_by_key(|p| std::cmp::Reverse(p.deletes.len() as isize - p.posts.len() as isize));
    let mut batches = Vec::new();
    let mut current = Batch::default();
    let size = |b: &Batch| b.deletes.len() + b.posts.len();
    for plan in plans {
        if plan.ops() <= MAX_BATCH_OPS {
            if size(&current) + plan.ops() > MAX_BATCH_OPS {
                batches.push(std::mem::take(&mut current));
            }
            current.deletes.extend(plan.deletes);
            current.posts.extend(plan.posts);
            continue;
        }
        for id in plan.deletes {
            if size(&current) == MAX_BATCH_OPS {
                batches.push(std::mem::take(&mut current));
            }
            current.deletes.push(id);
        }
        for post in plan.posts {
            if size(&current) == MAX_BATCH_OPS {
                batches.push(std::mem::take(&mut current));
            }
            current.posts.push(post);
        }
    }
    if size(&current) > 0 {
        batches.push(current);
    }
    batches
}

/// Runs a cycle every `cfg.interval`, starting once the snapshot has nodes. Stops with an
/// error when Cloudflare refuses the token, or never recognised the zone.
pub fn spawn(
    client: Client,
    cfg: PublishConfig,
    mut snapshots: SnapshotRx,
    shutdown: CancellationToken,
) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        cfg.validate()?;
        let mut rng = StdRng::from_rng(&mut rand::rng());
        // Fail fast on a wrong token or zone, before the crawler has found anything. A zone
        // error gets one retry: it is only fatal while nothing has ever worked.
        let apex = normalize_name(&cfg.apex.to_ascii());
        let mut ever_ok = false;
        let mut check = client.records_for(&apex).await;
        if matches!(check, Err(ApiError::Zone(_))) {
            tracing::warn!(retry_in = ?cfg.startup_retry, "Cloudflare did not recognise the zone; retrying once");
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = sleep(cfg.startup_retry) => {}
            }
            check = client.records_for(&apex).await;
        }
        match check {
            Err(e @ (ApiError::Auth(_) | ApiError::Zone(_))) => {
                return Err(anyhow::Error::new(e).context("publishing to Cloudflare"));
            }
            Err(e) => {
                tracing::warn!(error = %e, "could not verify the Cloudflare token; will retry")
            }
            Ok(_) => ever_ok = true,
        }
        let mut state = cfg
            .state_path
            .as_deref()
            .map_or_else(CycleState::default, CycleState::load);
        while snapshots.borrow().nodes.is_empty() {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                changed = snapshots.changed() => {
                    changed.context("the crawler stopped before publishing a snapshot")?;
                }
            }
        }
        let mut failures: u32 = 0;
        loop {
            let snapshot: Arc<Snapshot> = snapshots.borrow().clone();
            let result = reconcile(&client, &cfg, &snapshot, &mut rng, &mut state, &shutdown).await;
            if let Some(path) = &cfg.state_path {
                state.persist(path);
            }
            let wait = match result {
                Ok(report) => {
                    ever_ok |= !report.skipped_empty;
                    failures = 0;
                    cfg.interval
                }
                Err(e @ ApiError::Auth(_)) => {
                    return Err(anyhow::Error::new(e).context("publishing to Cloudflare"));
                }
                Err(e @ ApiError::Zone(_)) if !ever_ok => {
                    return Err(anyhow::Error::new(e).context("publishing to Cloudflare"));
                }
                // The zone worked before, so this is a Cloudflare-side fault: back off.
                Err(e) => {
                    failures = failures.saturating_add(1);
                    let wait = cfg
                        .interval
                        .saturating_mul(1u32 << (failures - 1).min(20))
                        .min(MAX_BACKOFF);
                    tracing::warn!(error = %e, retry_in = ?wait, "Cloudflare publish failed");
                    wait
                }
            };
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = sleep(wait) => {}
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: u64 = SHRINK_WINDOW.as_secs();

    #[test]
    fn high_water_mark_expires_after_24_hours() {
        let t0 = 1_000_000;
        let mut s = CycleState::default();
        assert_eq!(s.observe(t0, 25), None);
        assert_eq!(s.observe(t0 + 3600, 10), Some(25));
        // Past the window the old peak is forgotten.
        assert_eq!(s.observe(t0 + DAY + 3601, 10), None);
    }

    #[test]
    fn future_samples_are_dropped() {
        let mut s = CycleState::default();
        s.observe(10_000 + 3600, 100);
        assert_eq!(s.observe(10_000, 10), None);
    }

    #[derive(Clone, Default)]
    struct Logs(Arc<std::sync::Mutex<Vec<u8>>>);
    impl Write for Logs {
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

    fn logged<T>(f: impl FnOnce() -> T) -> (T, String) {
        let logs = Logs::default();
        let sub = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .finish();
        let out = tracing::subscriber::with_default(sub, f);
        let text = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        (out, text)
    }

    #[test]
    fn a_corrupt_file_warns_with_its_path_and_starts_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("g.json");
        std::fs::write(&path, "{nope").unwrap();
        let (s, text) = logged(|| CycleState::load_at(&path, 5_000));
        assert!(s.samples.is_empty());
        assert!(
            text.contains("corrupt") && text.contains("g.json"),
            "{text}"
        );
    }

    #[test]
    fn a_missing_file_is_silent() {
        let dir = tempfile::tempdir().unwrap();
        let (s, text) = logged(|| CycleState::load_at(&dir.path().join("none.json"), 5_000));
        assert!(s.samples.is_empty());
        assert!(text.is_empty(), "{text}");
    }

    #[test]
    fn future_samples_on_load_are_counted_in_a_warning() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("g.json");
        std::fs::write(&path, r#"{"samples": [[5000, 3], [9000, 4], [9100, 5]]}"#).unwrap();
        let (s, text) = logged(|| CycleState::load_at(&path, 5_100));
        assert_eq!(s.samples.len(), 1);
        assert!(text.contains("dropped=2"), "{text}");
    }

    #[test]
    fn samples_collapse_to_one_per_minute_keeping_the_max() {
        let mut s = CycleState::default();
        for i in 0..10u64 {
            s.observe(60_000 + i * 5, 10 + i as usize);
        }
        assert_eq!(s.samples, VecDeque::from([(60_000, 19)]));
        s.observe(60_060, 3);
        assert_eq!(s.samples.len(), 2);
    }

    #[test]
    fn the_file_never_exceeds_a_day_of_minutes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("g.json");
        let mut s = CycleState::default();
        let now = 10_000_000;
        for i in 0..3000u64 {
            s.observe(now - 3000 * 60 + i * 60, 5);
        }
        s.save(&path).unwrap();
        let n = CycleState::load_at(&path, now).samples.len();
        assert!(n <= MAX_SAMPLES, "{n}");
    }

    #[test]
    fn save_replaces_the_file_and_leaves_no_temp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("g.json");
        let mut s = CycleState::default();
        s.observe(5_000, 7);
        s.save(&path).unwrap();
        s.observe(5_100, 9);
        s.save(&path).unwrap();
        let loaded = CycleState::load_at(&path, 5_200);
        assert_eq!(loaded.samples, VecDeque::from([(5_000, 7), (5_100, 9)]));
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["g.json"]);
    }

    #[test]
    fn failed_save_keeps_the_old_file_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("g.json");
        let mut s = CycleState::default();
        s.observe(5_000, 7);
        s.save(&path).unwrap();
        // A directory in the target's place makes the rename fail.
        let blocked = dir.path().join("blocked");
        std::fs::create_dir(&blocked).unwrap();
        assert!(s.save(&blocked).is_err());
        assert!(!dir.path().join("blocked.tmp").exists());
        assert!(CycleState::load_at(&path, 5_100).samples.len() == 1);
    }

    fn plan(name: &str, deletes: usize, posts: usize) -> NamePlan {
        NamePlan {
            name: name.to_string(),
            deletes: (0..deletes).map(|i| format!("{name}-{i}")).collect(),
            posts: (0..posts)
                .map(|i| NewRecord {
                    name: name.to_string(),
                    kind: RecordKind::A,
                    content: IpAddr::from([203, 0, 113, (i % 250) as u8]),
                    ttl: 60,
                })
                .collect(),
            would_delete: Vec::new(),
        }
    }

    #[test]
    fn a_delete_heavy_plan_is_batched_before_a_post_heavy_one() {
        // Together they exceed one batch, so the order shows in which batch comes first.
        let batches = chunk(vec![plan("posty", 0, 150), plan("delety", 150, 0)]);
        assert_eq!(batches.len(), 2);
        assert_eq!((batches[0].deletes.len(), batches[0].posts.len()), (150, 0));
        assert_eq!((batches[1].deletes.len(), batches[1].posts.len()), (0, 150));
    }

    #[test]
    fn failed_save_in_a_read_only_directory_keeps_the_old_samples() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("g.json");
        let mut s = CycleState::default();
        s.observe(5_000, 7);
        s.save(&path).unwrap();
        s.observe(5_100, 9);
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
        // Root ignores directory permissions, so the scenario cannot be built.
        let is_root = std::fs::write(dir.path().join("probe"), "").is_ok();
        if !is_root {
            let result = s.save(&path);
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
            assert!(result.is_err(), "an in-place write would have succeeded");
            let loaded = CycleState::load_at(&path, 5_200);
            assert_eq!(loaded.samples, VecDeque::from([(5_000, 7)]));
        }
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}
