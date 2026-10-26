//! Counts the queries the authoritative server answers, per queried name, and writes them to
//! `<data_dir>/queries-<preset id>.json` once a minute. `doctor` reads the file as evidence
//! that real resolvers are reaching the seeder.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use hickory_proto::rr::Name;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cloudflare::api::normalize_name;

/// Distinct unpublished names kept per zone. Filter names are `x<hex>`, so a client can
/// invent many; past this they are folded into [`OTHER`].
const MAX_NAMES: usize = 1024;
/// The bucket for unpublished names over the cap.
pub const OTHER: &str = "_other";
pub const WRITE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameStats {
    /// Replies carrying at least one address.
    pub answered: u64,
    /// Valid replies with no address.
    pub empty: u64,
    /// Unix seconds of the latest query.
    pub last_seen: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryLog {
    /// Unix seconds when counting began.
    pub since: u64,
    /// Lowercase names without a trailing dot.
    pub names: BTreeMap<String, NameStats>,
}

/// The file for one network.
pub fn path_for(data_dir: &Path, preset_id: &str) -> PathBuf {
    data_dir.join(format!("queries-{preset_id}.json"))
}

/// Reads a query log written by the seeder.
pub fn read(path: &Path) -> Result<QueryLog> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read query log {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse query log {}", path.display()))
}

/// Counters for one zone, cheap to update from the serving loops.
pub struct Counters {
    inner: Mutex<Inner>,
}

struct Inner {
    log: QueryLog,
    /// The zone's published names: always counted, outside the cap.
    known: HashSet<String>,
    /// Entries in `log.names` that are neither known nor `OTHER`.
    extra: usize,
}

impl Counters {
    pub fn new(now: u64) -> Self {
        Self::with_known(now, std::iter::empty::<String>())
    }

    /// `known` are the names the seeder publishes (normalised, no trailing dot). They are
    /// counted whatever else arrives; other names are counted up to a cap, then folded
    /// into [`OTHER`].
    pub fn with_known(now: u64, known: impl IntoIterator<Item = String>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                log: QueryLog {
                    since: now,
                    names: BTreeMap::new(),
                },
                known: known.into_iter().collect(),
                extra: 0,
            }),
        }
    }

    pub fn record(&self, name: &str, answered: bool, now: u64) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let inner = &mut *guard;
        let stats = if let Some(stats) = inner.log.names.get_mut(name) {
            stats
        } else {
            let key = if inner.known.contains(name) {
                name
            } else if inner.extra < MAX_NAMES {
                inner.extra += 1;
                name
            } else {
                OTHER
            };
            inner.log.names.entry(key.to_string()).or_default()
        };
        if answered {
            stats.answered += 1;
        } else {
            stats.empty += 1;
        }
        stats.last_seen = now;
    }

    pub fn snapshot(&self) -> QueryLog {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .log
            .clone()
    }
}

/// The counters of every authoritative zone, found by the queried name.
#[derive(Default)]
pub struct QueryLogs(Vec<(Name, Arc<Counters>)>);

impl QueryLogs {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, apex: Name, counters: Arc<Counters>) {
        self.0.push((apex, counters));
    }

    pub fn record(&self, qname: &Name, answered: bool) {
        let zone = self
            .0
            .iter()
            .filter(|(apex, _)| apex.zone_of(qname))
            .max_by_key(|(apex, _)| apex.iter().count());
        if let Some((_, counters)) = zone {
            counters.record(&normalize_name(&qname.to_ascii()), answered, unix_now());
        }
    }
}

/// Writes `log` to `path` through a temporary file in the same directory and a rename, so a
/// reader sees the old file or the new one, never half of one.
pub fn write_atomic(path: &Path, log: &QueryLog) -> Result<()> {
    let file_name = path
        .file_name()
        .with_context(|| format!("query log path {} has no file name", path.display()))?;
    let mut tmp_name = std::ffi::OsString::from(".");
    tmp_name.push(file_name);
    tmp_name.push(".tmp");
    let tmp = path.with_file_name(tmp_name);
    let result = std::fs::write(&tmp, serde_json::to_vec(log)?)
        .and_then(|()| std::fs::rename(&tmp, path))
        .with_context(|| format!("write query log {}", path.display()));
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// Writes the counters every `interval` and once more when `shutdown` fires.
pub fn spawn_writer(
    counters: Arc<Counters>,
    path: PathBuf,
    interval: Duration,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let write = || {
            if let Err(e) = write_atomic(&path, &counters.snapshot()) {
                tracing::warn!("{e:#}");
            }
        };
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tick.tick() => write(),
            }
        }
        write();
    })
}

pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_answered_and_empty_per_name() {
        let c = Counters::new(100);
        c.record("x1.seed.example", true, 110);
        c.record("x1.seed.example", true, 120);
        c.record("x1.seed.example", false, 130);
        c.record("seed.example", false, 140);
        let log = c.snapshot();
        assert_eq!(log.since, 100);
        assert_eq!(
            log.names["x1.seed.example"],
            NameStats {
                answered: 2,
                empty: 1,
                last_seen: 130
            }
        );
        assert_eq!(log.names["seed.example"].empty, 1);
        assert_eq!(log.names.len(), 2);
    }

    #[test]
    fn logs_route_to_the_longest_matching_zone_and_ignore_others() {
        let name = |s: &str| Name::from_ascii(s).unwrap();
        let (outer, inner) = (Arc::new(Counters::new(1)), Arc::new(Counters::new(1)));
        let mut logs = QueryLogs::new();
        logs.add(name("seed.example."), outer.clone());
        logs.add(name("a.seed.example."), inner.clone());
        logs.record(&name("X1.A.Seed.Example."), true);
        logs.record(&name("x1.seed.example."), false);
        logs.record(&name("other.example."), true);
        assert!(inner.snapshot().names.contains_key("x1.a.seed.example"));
        assert_eq!(outer.snapshot().names.len(), 1);
        assert_eq!(outer.snapshot().names["x1.seed.example"].empty, 1);
    }

    #[test]
    fn junk_names_cannot_blank_the_real_ones() {
        let c = Counters::with_known(1, ["x1.seed.example".to_string()]);
        for i in 0..MAX_NAMES + 50 {
            c.record(&format!("xa{i:x}.seed.example"), true, 2);
        }
        c.record("x1.seed.example", true, 3);
        c.record("x1.seed.example", false, 4);
        c.record("xa0.seed.example", true, 5);
        let log = c.snapshot();
        assert_eq!(log.names["x1.seed.example"].answered, 1);
        assert_eq!(log.names["x1.seed.example"].empty, 1);
        assert_eq!(log.names[OTHER].answered, 50);
        assert_eq!(log.names["xa0.seed.example"].answered, 2);
        assert_eq!(log.names.len(), MAX_NAMES + 2);
    }

    #[test]
    fn file_written_atomically() {
        let d = tempfile::tempdir().unwrap();
        let path = path_for(d.path(), "osmium-mainnet");
        std::fs::write(&path, "old").unwrap();
        let c = Counters::new(5);
        c.record("x1.seed.example", true, 9);
        write_atomic(&path, &c.snapshot()).unwrap();
        // The final path holds a complete document and no temporary file is left behind.
        let log = read(&path).unwrap();
        assert_eq!(log.since, 5);
        assert_eq!(log.names["x1.seed.example"].answered, 1);
        let files: Vec<_> = std::fs::read_dir(d.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(files, ["queries-osmium-mainnet.json"]);

        // A failed rename (the target is a directory) cleans up its temporary file and
        // leaves the target alone.
        let blocked = d.path().join("queries-blocked.json");
        std::fs::create_dir(&blocked).unwrap();
        assert!(write_atomic(&blocked, &c.snapshot()).is_err());
        assert!(blocked.is_dir());
        assert_eq!(std::fs::read_dir(d.path()).unwrap().count(), 2);
    }

    #[test]
    fn a_write_that_cannot_create_its_temporary_file_leaves_the_old_log() {
        use std::os::unix::fs::PermissionsExt;

        let d = tempfile::tempdir().unwrap();
        let path = path_for(d.path(), "osmium-mainnet");
        std::fs::write(&path, "old").unwrap();
        std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
        // Root ignores directory permissions, so the scenario cannot be built.
        let probe = d.path().join("probe");
        let is_root = std::fs::write(&probe, "").is_ok();
        if !is_root {
            let c = Counters::new(5);
            c.record("x1.seed.example", true, 9);
            let result = write_atomic(&path, &c.snapshot());
            let after = std::fs::read_to_string(&path);
            std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
            assert!(result.is_err(), "an in-place write would have succeeded");
            assert_eq!(after.unwrap(), "old");
        }
        std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn json_shape_matches_the_documented_format() {
        let c = Counters::new(5);
        c.record("x1.seed.example", true, 9);
        let v: serde_json::Value = serde_json::to_value(c.snapshot()).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"since": 5, "names": {"x1.seed.example":
                {"answered": 1, "empty": 0, "last_seen": 9}}})
        );
    }

    #[test]
    fn read_reports_a_missing_or_broken_file() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("q.json");
        assert!(format!("{:#}", read(&p).unwrap_err()).contains("q.json"));
        std::fs::write(&p, "{").unwrap();
        assert!(format!("{:#}", read(&p).unwrap_err()).contains("q.json"));
    }

    #[tokio::test]
    async fn writer_writes_on_the_interval_and_once_more_at_shutdown() {
        let d = tempfile::tempdir().unwrap();
        let path = path_for(d.path(), "p");
        let c = Arc::new(Counters::new(1));
        let stop = CancellationToken::new();
        let h = spawn_writer(
            c.clone(),
            path.clone(),
            Duration::from_millis(50),
            stop.clone(),
        );
        c.record("a.seed.example", true, 2);
        let end = std::time::Instant::now() + Duration::from_secs(5);
        while read(&path).map_or(true, |l| l.names.is_empty()) {
            assert!(std::time::Instant::now() < end, "never written");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Counted after the last tick, found by the shutdown write.
        let long = path_for(d.path(), "q");
        let h2 = spawn_writer(
            c.clone(),
            long.clone(),
            Duration::from_secs(3600),
            stop.clone(),
        );
        c.record("b.seed.example", false, 3);
        stop.cancel();
        h.await.unwrap();
        h2.await.unwrap();
        assert_eq!(read(&long).unwrap().names["b.seed.example"].empty, 1);
        assert!(read(&path).unwrap().names.contains_key("b.seed.example"));
    }
}
