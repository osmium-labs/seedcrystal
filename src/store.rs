//! SQLite node store: what the crawler has learned and what DNS may serve.
//!
//! Synchronous; async callers wrap it in `spawn_blocking`. Times are unix
//! seconds supplied by the caller, never read from the clock here.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, params};

use crate::coin::{VersionStep, serve_floor};
use crate::p2p::ChainCheck;
use crate::p2p::messages::{AddrEntry, NetAddr};
use crate::score::Stats;

const SCHEMA_VERSION: i64 = 2;
const GOOD_RETRY: i64 = 15 * 60;
const BAD_RETRY_MIN: i64 = 5 * 60;
const BAD_RETRY_MAX: i64 = 24 * 3600;
const HARVEST_INTERVAL: i64 = 24 * 3600;
const TIP_WINDOW: i64 = 2 * 3600;
const WARMUP_WINDOW: i64 = 3600;
/// `nodes.chain`: 0 is unknown.
const CHAIN_VERIFIED: i64 = 1;
const CHAIN_WRONG: i64 = 2;

pub struct NodeStore {
    conn: Mutex<Connection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Good {
        version: i32,
        services: u64,
        height: i32,
        harvested: bool,
        chain: ChainCheck,
    },
    Bad,
    /// The peer answered, but its headers do not build on the checkpoint.
    WrongChain {
        harvested: bool,
    },
}

/// A node to probe now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Due {
    pub addr: SocketAddr,
    /// Also ask for addresses (`getaddr`).
    pub harvest: bool,
    /// Also run the checkpoint probe.
    pub check_chain: bool,
}

#[derive(Debug, Clone)]
pub struct ServablePolicy {
    /// `Some(p)`: the node's port must equal `p`. `None` accepts any port; it
    /// is a test/LAN relaxation that the crawler uses only together with
    /// `allow_private`.
    pub port: Option<u16>,
    /// The lowest serve floor at any height; the tip query filters on it.
    pub serve_floor_base: i32,
    pub schedule: Vec<VersionStep>,
    pub min_height: i32,
    pub tip_tolerance: i32,
    pub warmup_floor: usize,
    pub allow_private: bool,
    /// The preset checkpoint's display hash; when set, only nodes verified
    /// under exactly this checkpoint are servable.
    pub checkpoint_ref: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Servable {
    pub addr: SocketAddr,
    pub services: u64,
}

/// Collapse IPv4-mapped IPv6 (`::ffff:a.b.c.d`) to plain IPv4.
fn normalise(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        v4 => v4,
    }
}

fn routable_v4(a: Ipv4Addr) -> bool {
    let o = a.octets();
    !(a.is_loopback()
        || a.is_unspecified()
        || a.is_private()
        || a.is_link_local()
        || a.is_multicast()
        || a.is_broadcast()
        || a.is_documentation()
        || o[0] == 0
        || o[0] >= 240
        || (o[0] == 100 && (o[1] & 0xc0) == 64)
        || (o[0] == 198 && (o[1] & 0xfe) == 18))
}

fn routable_v6(a: Ipv6Addr) -> bool {
    let s = a.segments();
    !(a.is_loopback()
        || a.is_unspecified()
        || a.is_multicast()
        || (s[0] & 0xfe00) == 0xfc00
        || (s[0] & 0xffc0) == 0xfe80
        || (s[0] == 0x2001 && s[1] == 0x0db8))
}

/// Whether an address is a plausible public peer.
pub fn is_routable(ip: IpAddr) -> bool {
    match normalise(ip) {
        IpAddr::V4(a) => routable_v4(a),
        IpAddr::V6(a) => routable_v6(a),
    }
}

fn parse_row_addr(ip: &str, port: i64) -> Option<SocketAddr> {
    Some(SocketAddr::new(ip.parse().ok()?, u16::try_from(port).ok()?))
}

#[derive(Clone, Copy)]
enum DueKind {
    GoodRetry,
    New,
    BadRetry,
}

/// The `due` query for one group. `INDEXED BY` pins the partial index that
/// matches the filter and the ORDER BY, so a limit stops after `LIMIT` rows
/// rather than scanning past unroutable ones.
fn due_sql(kind: DueKind, allow_private: bool) -> String {
    let (suffix, route) = if allow_private {
        ("any", "")
    } else {
        ("pub", "AND routable = 1")
    };
    let (index, filter, order) = match kind {
        DueKind::GoodRetry => (
            "good",
            "tried = 1 AND last_ok = 1 AND next_try <= ?1",
            "next_try",
        ),
        DueKind::New => ("new", "tried = 0", "first_seen"),
        DueKind::BadRetry => (
            "bad",
            "tried = 1 AND last_ok = 0 AND next_try <= ?1",
            "next_try",
        ),
    };
    format!(
        "SELECT ip, port, (last_harvest = 0 OR ?1 - last_harvest >= ?2), chain, chain_ref \
         FROM nodes \
         INDEXED BY nodes_{index}_{suffix} WHERE {filter} {route} ORDER BY {order} LIMIT ?3"
    )
}

impl NodeStore {
    /// Opens or creates the store; a store newer than this build is refused.
    pub fn open(path: &Path) -> Result<NodeStore> {
        let conn = Connection::open(path)
            .with_context(|| format!("opening node store {}", path.display()))?;
        conn.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version > SCHEMA_VERSION {
            anyhow::bail!(
                "node store {} has schema version {version}, newer than this build supports ({SCHEMA_VERSION})",
                path.display()
            );
        }
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS nodes (
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                routable INTEGER NOT NULL,
                services INTEGER NOT NULL DEFAULT 0,
                version INTEGER NOT NULL DEFAULT 0,
                height INTEGER NOT NULL DEFAULT 0,
                stats TEXT NOT NULL DEFAULT '',
                last_ok INTEGER NOT NULL DEFAULT 0,
                last_good INTEGER NOT NULL DEFAULT 0,
                tried INTEGER NOT NULL DEFAULT 0,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                next_try INTEGER NOT NULL DEFAULT 0,
                last_harvest INTEGER NOT NULL DEFAULT 0,
                first_seen INTEGER NOT NULL,
                chain INTEGER NOT NULL DEFAULT 0,
                chain_ref TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (ip, port)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS nodes_new_pub ON nodes (first_seen) WHERE tried = 0 AND routable = 1;
            CREATE INDEX IF NOT EXISTS nodes_good_pub ON nodes (next_try) WHERE tried = 1 AND last_ok = 1 AND routable = 1;
            CREATE INDEX IF NOT EXISTS nodes_bad_pub ON nodes (next_try) WHERE tried = 1 AND last_ok = 0 AND routable = 1;
            CREATE INDEX IF NOT EXISTS nodes_new_any ON nodes (first_seen) WHERE tried = 0;
            CREATE INDEX IF NOT EXISTS nodes_good_any ON nodes (next_try) WHERE tried = 1 AND last_ok = 1;
            CREATE INDEX IF NOT EXISTS nodes_bad_any ON nodes (next_try) WHERE tried = 1 AND last_ok = 0;",
        )?;
        conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        Ok(NodeStore {
            conn: Mutex::new(conn),
        })
    }

    /// Opens an existing store for reading, for reports. It never creates the file or runs DDL:
    /// a store at any other schema version is refused, so a report cannot write the live
    /// database while the service owns it. `query_only` refuses every
    /// write. The connection is read-write only so that, as the last one to close, it removes
    /// the `-wal` and `-shm` files it opened; a read-only connection would leave them behind.
    pub fn open_existing(path: &Path) -> Result<NodeStore> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("opening node store {}", path.display()))?;
        conn.pragma_update(None, "query_only", true)?;
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .with_context(|| format!("reading node store {}", path.display()))?;
        if version != SCHEMA_VERSION {
            anyhow::bail!(
                "node store {} is schema {version}; this build reads schema {SCHEMA_VERSION}",
                path.display()
            );
        }
        Ok(NodeStore {
            conn: Mutex::new(conn),
        })
    }

    fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Store newly learned V4/V6 addresses; returns the rows actually inserted.
    ///
    /// With `accept_port: Some(p)`, entries on any other port are dropped; `None`
    /// accepts any port and is meant only for use with `allow_private`. Port 0
    /// is always dropped.
    pub fn add_learned(
        &self,
        entries: &[AddrEntry],
        now: i64,
        accept_port: Option<u16>,
    ) -> Result<usize> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mut inserted = 0;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO nodes (ip, port, routable, services, first_seen)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for e in entries {
                if e.port == 0 || accept_port.is_some_and(|p| p != e.port) {
                    continue;
                }
                let ip = match e.addr {
                    NetAddr::V4(a) => IpAddr::V4(a),
                    NetAddr::V6(a) => normalise(IpAddr::V6(a)),
                    _ => continue,
                };
                inserted += stmt.execute(params![
                    ip.to_string(),
                    e.port,
                    is_routable(ip),
                    e.services as i64,
                    now
                ])?;
            }
        }
        tx.commit()?;
        Ok(inserted)
    }

    /// Records one probe. `checkpoint_ref` is the display hash of the preset's checkpoint;
    /// a verified or wrong-chain verdict is stored together with it. Other chain checks
    /// leave the stored verdict as it is.
    pub fn record(
        &self,
        addr: SocketAddr,
        outcome: Outcome,
        now: i64,
        checkpoint_ref: Option<&str>,
    ) -> Result<()> {
        let ip = normalise(addr.ip());
        let ip_s = ip.to_string();
        let port = addr.port();
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO nodes (ip, port, routable, first_seen) VALUES (?1, ?2, ?3, ?4)",
            params![ip_s, port, is_routable(ip), now],
        )?;
        let (stats_s, failures): (String, i64) = tx.query_row(
            "SELECT stats, consecutive_failures FROM nodes WHERE ip = ?1 AND port = ?2",
            params![ip_s, port],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut stats = Stats::decode(&stats_s).unwrap_or_default();
        let verdict = match outcome {
            Outcome::Good {
                chain: ChainCheck::Verified,
                ..
            } => Some(CHAIN_VERIFIED),
            Outcome::WrongChain { .. } => Some(CHAIN_WRONG),
            _ => None,
        };
        match outcome {
            Outcome::Good {
                version,
                services,
                height,
                harvested,
                ..
            } => {
                stats.update(true, now);
                tx.execute(
                    "UPDATE nodes SET services = ?3, version = ?4, height = ?5, stats = ?6,
                         last_ok = 1, last_good = ?7, tried = 1, consecutive_failures = 0,
                         next_try = ?8,
                         last_harvest = CASE WHEN ?9 THEN ?7 ELSE last_harvest END
                     WHERE ip = ?1 AND port = ?2",
                    params![
                        ip_s,
                        port,
                        services as i64,
                        version,
                        height,
                        stats.encode(),
                        now,
                        now + GOOD_RETRY,
                        harvested
                    ],
                )?;
            }
            Outcome::Bad | Outcome::WrongChain { .. } => {
                // A wrong-chain probe that harvested counts as a harvest, so its back-off
                // re-probes do not ask again.
                let harvested = matches!(outcome, Outcome::WrongChain { harvested: true });
                stats.update(false, now);
                let failures = failures + 1;
                let backoff =
                    (BAD_RETRY_MIN << failures.min(20)).clamp(BAD_RETRY_MIN, BAD_RETRY_MAX);
                tx.execute(
                    "UPDATE nodes SET stats = ?3, last_ok = 0, tried = 1,
                         consecutive_failures = ?4, next_try = ?5,
                         last_harvest = CASE WHEN ?7 THEN ?6 ELSE last_harvest END
                     WHERE ip = ?1 AND port = ?2",
                    params![
                        ip_s,
                        port,
                        stats.encode(),
                        failures,
                        now + backoff,
                        now,
                        harvested
                    ],
                )?;
            }
        }
        if let (Some(chain), Some(reference)) = (verdict, checkpoint_ref) {
            tx.execute(
                "UPDATE nodes SET chain = ?3, chain_ref = ?4 WHERE ip = ?1 AND port = ?2",
                params![ip_s, port, chain, reference],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Nodes to probe now, within `limit`: first due re-checks of nodes whose
    /// last probe succeeded (oldest `next_try` first, so served nodes stay
    /// fresh), then the remaining budget split evenly between never-tried
    /// addresses (oldest `first_seen`) and other due retries (oldest
    /// `next_try`); a group that cannot use its share gives it to the other.
    /// Each entry says whether to also harvest (`getaddr`) and whether to check the chain:
    /// on every harvest, and whenever the node is not verified under `checkpoint_ref`.
    pub fn due(
        &self,
        now: i64,
        limit: usize,
        allow_private: bool,
        checkpoint_ref: Option<&str>,
    ) -> Result<Vec<Due>> {
        let conn = self.conn();
        let good = due_sql(DueKind::GoodRetry, allow_private);
        let never = due_sql(DueKind::New, allow_private);
        let bad = due_sql(DueKind::BadRetry, allow_private);
        let fetch = |sql: &str, n: usize| -> Result<Vec<Due>> {
            if n == 0 {
                return Ok(Vec::new());
            }
            let mut stmt = conn.prepare_cached(sql)?;
            let rows = stmt.query_map(params![now, HARVEST_INTERVAL, n as i64], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, bool>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (ip, port, harvest, chain, chain_ref) = row?;
                let verified =
                    chain == CHAIN_VERIFIED && checkpoint_ref == Some(chain_ref.as_str());
                if let Some(a) = parse_row_addr(&ip, port) {
                    out.push(Due {
                        addr: a,
                        harvest,
                        check_chain: harvest || !verified,
                    });
                }
            }
            Ok(out)
        };
        let mut out = fetch(&good, limit)?;
        let rest = limit.saturating_sub(out.len());
        let share = rest.div_ceil(2);
        let mut fresh = fetch(&never, share)?;
        let retries = fetch(&bad, rest - fresh.len())?;
        if fresh.len() == share && fresh.len() + retries.len() < rest {
            fresh = fetch(&never, rest - retries.len())?;
        }
        out.extend(fresh);
        out.extend(retries);
        Ok(out)
    }

    /// Network tip as seen by this store: the 3rd-highest height among nodes
    /// that pass every non-height servable check (a verified chain among them,
    /// when a checkpoint is set) and were good within 2 h
    /// (the maximum if fewer than 3 qualify, 0 if none). One node lying about
    /// its height therefore cannot move the tip.
    pub fn tip(&self, policy: &ServablePolicy, now: i64) -> Result<i32> {
        let conn = self.conn();
        let mut stmt = conn.prepare_cached(
            "SELECT height FROM nodes
             WHERE last_ok = 1 AND last_good >= ?1 AND version >= ?2
               AND (routable = 1 OR ?3) AND (?4 IS NULL OR port = ?4)
               AND (?5 IS NULL OR (chain = 1 AND chain_ref = ?5))
             ORDER BY height DESC LIMIT 3",
        )?;
        let heights: Vec<i32> = stmt
            .query_map(
                params![
                    now - TIP_WINDOW,
                    policy.serve_floor_base,
                    policy.allow_private,
                    policy.port,
                    policy.checkpoint_ref
                ],
                |r| r.get(0),
            )?
            .collect::<rusqlite::Result<_>>()?;
        let tip = if heights.len() < 3 {
            heights.first()
        } else {
            heights.last()
        };
        Ok(tip.copied().unwrap_or(0))
    }

    pub fn servable(&self, policy: &ServablePolicy, now: i64) -> Result<Vec<Servable>> {
        let tip = self.tip(policy, now)?;
        let floor_version = serve_floor(policy.serve_floor_base, &policy.schedule, tip);
        let floor_height =
            (i64::from(tip) - i64::from(policy.tip_tolerance)).max(i64::from(policy.min_height));
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT ip, port, services, stats, last_good FROM nodes
             WHERE last_ok = 1 AND version >= ?1 AND height >= ?2
               AND (routable = 1 OR ?3) AND (?4 IS NULL OR port = ?4)
               AND (?5 IS NULL OR (chain = 1 AND chain_ref = ?5))
             ORDER BY ip, port",
        )?;
        let rows = stmt.query_map(
            params![
                floor_version,
                floor_height,
                policy.allow_private,
                policy.port,
                policy.checkpoint_ref
            ],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            },
        )?;
        let mut full = Vec::new();
        let mut recent = Vec::new();
        for row in rows {
            let (ip, port, services, stats, last_good) = row?;
            let Some(addr) = parse_row_addr(&ip, port) else {
                continue;
            };
            let node = Servable {
                addr,
                services: services as u64,
            };
            if Stats::decode(&stats).is_some_and(|s| s.meets_full_bar()) {
                full.push(node);
            } else if last_good >= now - WARMUP_WINDOW {
                recent.push(node);
            }
        }
        if full.len() < policy.warmup_floor {
            full.extend(recent);
        }
        Ok(full)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sa(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    /// A due entry with no checkpoint in force, where every probe checks the chain.
    fn due_at(addr: &str, harvest: bool) -> Due {
        Due {
            addr: sa(addr),
            harvest,
            check_chain: true,
        }
    }

    fn entry(ip: &str, port: u16) -> AddrEntry {
        let addr = match ip.parse::<IpAddr>().unwrap() {
            IpAddr::V4(a) => NetAddr::V4(a),
            IpAddr::V6(a) => NetAddr::V6(a),
        };
        AddrEntry {
            addr,
            port,
            services: 1,
            time: 0,
        }
    }

    fn good(height: i32) -> Outcome {
        Outcome::Good {
            version: 70216,
            services: 1,
            height,
            harvested: false,
            chain: ChainCheck::NotChecked,
        }
    }

    fn policy() -> ServablePolicy {
        ServablePolicy {
            port: Some(8080),
            serve_floor_base: 70215,
            schedule: Vec::new(),
            min_height: 0,
            tip_tolerance: 100,
            warmup_floor: 10,
            allow_private: false,
            checkpoint_ref: None,
        }
    }

    fn open() -> (tempfile::TempDir, NodeStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = NodeStore::open(&dir.path().join("nodes.db")).unwrap();
        (dir, store)
    }

    /// Three successes 10 minutes apart: meets the full bar.
    fn make_full_bar(store: &NodeStore, addr: SocketAddr, t: i64) {
        for i in 0..3 {
            store.record(addr, good(1000), t + i * 600, None).unwrap();
        }
    }

    #[test]
    fn servable_filters_port_version_height_and_privacy() {
        let (_d, store) = open();
        let t = 10_000;
        store
            .record(sa("8.8.8.1:9999"), good(1000), t, None)
            .unwrap();
        store
            .record(
                sa("8.8.8.2:8080"),
                Outcome::Good {
                    version: 70214,
                    services: 1,
                    height: 1000,
                    harvested: false,
                    chain: ChainCheck::NotChecked,
                },
                t,
                None,
            )
            .unwrap();
        store
            .record(sa("8.8.8.3:8080"), good(850), t, None)
            .unwrap();
        store
            .record(sa("10.0.0.5:8080"), good(1000), t, None)
            .unwrap();
        store
            .record(sa("8.8.8.4:8080"), good(1000), t, None)
            .unwrap();
        store
            .record(sa("8.8.8.5:8080"), good(900), t, None)
            .unwrap();
        store
            .record(sa("8.8.8.6:8080"), good(1000), t, None)
            .unwrap();
        store
            .record(sa("8.8.8.7:8080"), good(1000), t, None)
            .unwrap();
        let mut got: Vec<_> = store
            .servable(&policy(), t + 10)
            .unwrap()
            .into_iter()
            .map(|s| s.addr)
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                sa("8.8.8.4:8080"),
                sa("8.8.8.5:8080"),
                sa("8.8.8.6:8080"),
                sa("8.8.8.7:8080")
            ]
        );
        let mut p = policy();
        p.allow_private = true;
        assert!(
            store
                .servable(&p, t + 10)
                .unwrap()
                .iter()
                .any(|s| s.addr == sa("10.0.0.5:8080"))
        );
        p.port = None;
        assert!(
            store
                .servable(&p, t + 10)
                .unwrap()
                .iter()
                .any(|s| s.addr == sa("8.8.8.1:9999"))
        );
    }

    fn ver(version: i32, height: i32) -> Outcome {
        Outcome::Good {
            version,
            services: 1,
            height,
            harvested: false,
            chain: ChainCheck::NotChecked,
        }
    }

    fn addrs(store: &NodeStore, p: &ServablePolicy, now: i64) -> Vec<SocketAddr> {
        store
            .servable(p, now)
            .unwrap()
            .into_iter()
            .map(|s| s.addr)
            .collect()
    }

    #[test]
    fn schedule_raises_the_serve_floor_at_the_tip() {
        let (_d, store) = open();
        for (a, v) in [("8.8.8.1:8080", 70012), ("8.8.8.2:8080", 70015)] {
            store.record(sa(a), ver(v, 250), 100, None).unwrap();
        }
        let p = ServablePolicy {
            serve_floor_base: 70012,
            schedule: vec![
                VersionStep {
                    height: 100,
                    version: 70012,
                },
                VersionStep {
                    height: 200,
                    version: 70015,
                },
            ],
            ..policy()
        };
        assert_eq!(addrs(&store, &p, 110), vec![sa("8.8.8.2:8080")]);

        let (_d, low) = open();
        for (a, v) in [("8.8.8.1:8080", 70012), ("8.8.8.2:8080", 70015)] {
            low.record(sa(a), ver(v, 150), 100, None).unwrap();
        }
        assert_eq!(
            addrs(&low, &p, 110),
            vec![sa("8.8.8.1:8080"), sa("8.8.8.2:8080")]
        );
    }

    #[test]
    fn min_height_is_an_absolute_floor() {
        let (_d, store) = open();
        for (a, h) in [
            ("8.8.8.1:8080", 50),
            ("8.8.8.2:8080", 60),
            ("8.8.8.3:8080", 70),
        ] {
            store.record(sa(a), good(h), 100, None).unwrap();
        }
        let p = ServablePolicy {
            min_height: 65,
            ..policy()
        };
        assert_eq!(addrs(&store, &p, 110), vec![sa("8.8.8.3:8080")]);
    }

    #[test]
    fn tolerance_comes_from_the_policy() {
        let (_d, store) = open();
        for (a, h) in [
            ("8.8.8.1:8080", 1000),
            ("8.8.8.2:8080", 1000),
            ("8.8.8.3:8080", 1000),
            ("8.8.8.4:8080", 990),
            ("8.8.8.5:8080", 900),
        ] {
            store.record(sa(a), good(h), 100, None).unwrap();
        }
        let tight = ServablePolicy {
            tip_tolerance: 12,
            ..policy()
        };
        assert_eq!(
            addrs(&store, &tight, 110),
            vec![
                sa("8.8.8.1:8080"),
                sa("8.8.8.2:8080"),
                sa("8.8.8.3:8080"),
                sa("8.8.8.4:8080")
            ]
        );
        assert_eq!(addrs(&store, &policy(), 110).len(), 5);
    }

    #[test]
    fn tip_rule_is_unchanged_below_three_nodes() {
        let (_d, store) = open();
        store
            .record(sa("8.8.8.1:8080"), good(1000), 100, None)
            .unwrap();
        store
            .record(sa("8.8.8.2:8080"), good(0), 100, None)
            .unwrap();
        assert_eq!(store.tip(&policy(), 110).unwrap(), 1000);
        assert_eq!(addrs(&store, &policy(), 110), vec![sa("8.8.8.1:8080")]);
    }

    #[test]
    fn version_below_minimum_is_excluded() {
        let (_d, store) = open();
        store
            .record(
                sa("8.8.8.2:8080"),
                Outcome::Good {
                    version: 70214,
                    services: 1,
                    height: 1000,
                    harvested: false,
                    chain: ChainCheck::NotChecked,
                },
                100,
                None,
            )
            .unwrap();
        assert!(store.servable(&policy(), 110).unwrap().is_empty());
    }

    #[test]
    fn last_outcome_bad_is_not_served() {
        let (_d, store) = open();
        let a = sa("8.8.8.1:8080");
        store.record(a, good(1000), 100, None).unwrap();
        assert_eq!(store.servable(&policy(), 110).unwrap().len(), 1);
        store.record(a, Outcome::Bad, 200, None).unwrap();
        assert!(store.servable(&policy(), 210).unwrap().is_empty());
    }

    #[test]
    fn warmup_serves_single_handshake_nodes_until_floor() {
        let (_d, store) = open();
        let t = 50_000;
        for i in 1..=3 {
            store
                .record(sa(&format!("8.8.8.{i}:8080")), good(1000), t, None)
                .unwrap();
        }
        assert_eq!(store.servable(&policy(), t + 10).unwrap().len(), 3);
        for i in 11..=20 {
            make_full_bar(&store, sa(&format!("8.8.8.{i}:8080")), t);
        }
        let got = store.servable(&policy(), t + 1300).unwrap();
        assert_eq!(got.len(), 10);
        let want: std::collections::HashSet<SocketAddr> =
            (11..=20).map(|i| sa(&format!("8.8.8.{i}:8080"))).collect();
        let got: std::collections::HashSet<SocketAddr> = got.iter().map(|s| s.addr).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn warmup_ignores_stale_single_handshake_nodes() {
        let (_d, store) = open();
        store
            .record(sa("8.8.8.1:8080"), good(1000), 0, None)
            .unwrap();
        assert!(store.servable(&policy(), 3601).unwrap().is_empty());
        assert_eq!(store.servable(&policy(), 3600).unwrap().len(), 1);
    }

    #[test]
    fn tip_uses_recent_good_nodes_only() {
        let (_d, store) = open();
        let p = policy();
        assert_eq!(store.tip(&p, 0).unwrap(), 0);
        store
            .record(sa("8.8.8.1:8080"), good(500), 0, None)
            .unwrap();
        store
            .record(sa("8.8.8.2:8080"), good(900), 5000, None)
            .unwrap();
        // Two qualifying nodes: the tip is the maximum.
        assert_eq!(store.tip(&p, 5000).unwrap(), 900);
        assert_eq!(store.tip(&p, 7200 + 5001).unwrap(), 0);
        assert_eq!(store.tip(&p, 7200).unwrap(), 900);
    }

    #[test]
    fn tip_ignores_nodes_whose_last_probe_failed() {
        let (_d, store) = open();
        let t = 100;
        for i in 0..3 {
            store
                .record(sa(&format!("8.8.8.{}:8080", i + 1)), good(1000), t, None)
                .unwrap();
        }
        // Three liars at a huge height were good once, then failed: still recent, not ok.
        for i in 0..3 {
            let liar = sa(&format!("8.8.9.{}:8080", i + 1));
            store.record(liar, good(2000 + i), t, None).unwrap();
            store.record(liar, Outcome::Bad, t + 10, None).unwrap();
        }
        assert_eq!(store.tip(&policy(), t + 20).unwrap(), 1000);
    }

    #[test]
    fn tip_counts_only_nodes_verified_under_the_current_checkpoint() {
        let (_d, store) = open();
        let t = 100;
        let verified = |store: &NodeStore, ip: &str, height: i32, reference: &str| {
            let outcome = Outcome::Good {
                version: 70216,
                services: 1,
                height,
                harvested: false,
                chain: ChainCheck::Verified,
            };
            store.record(sa(ip), outcome, t, Some(reference)).unwrap();
        };
        for i in 0..3 {
            verified(&store, &format!("8.8.8.{}:8080", i + 1), 1000, "A");
            verified(&store, &format!("8.8.7.{}:8080", i + 1), 8_000_000, "B");
            store
                .record(
                    sa(&format!("8.8.9.{}:8080", i + 1)),
                    good(9_000_000),
                    t,
                    Some("A"),
                )
                .unwrap();
        }
        let under = |r: &str| ServablePolicy {
            checkpoint_ref: Some(r.into()),
            ..policy()
        };
        assert_eq!(store.tip(&under("A"), t).unwrap(), 1000);
        assert_eq!(store.tip(&under("B"), t).unwrap(), 8_000_000);
        assert_eq!(store.tip(&policy(), t).unwrap(), 9_000_000);
    }

    #[test]
    fn tip_window_is_two_hours_on_both_sides() {
        let (_d, store) = open();
        let t = 1_000_000;
        for i in 0..3 {
            store
                .record(
                    sa(&format!("8.8.8.{}:8080", i + 1)),
                    good(1000),
                    t - 3700,
                    None,
                )
                .unwrap();
            store
                .record(sa(&format!("8.8.9.{}:8080", i + 1)), good(900), t, None)
                .unwrap();
        }
        // The older nodes are 3700 s old: inside the 7200 s window.
        assert_eq!(store.tip(&policy(), t).unwrap(), 1000);
        // At exactly 7200 s they still count; one second later they do not.
        assert_eq!(store.tip(&policy(), t + 3500).unwrap(), 1000);
        assert_eq!(store.tip(&policy(), t + 3501).unwrap(), 900);
    }

    #[test]
    fn tip_is_third_highest_and_ignores_unqualified_nodes() {
        let (_d, store) = open();
        let t = 100;
        for (i, h) in [1000, 1001, 1002, 999, 1000].iter().enumerate() {
            store
                .record(sa(&format!("8.8.8.{}:8080", i + 1)), good(*h), t, None)
                .unwrap();
        }
        assert_eq!(store.tip(&policy(), t).unwrap(), 1000);
        // Unqualified liars (wrong port, private, old version) do not count.
        store
            .record(sa("8.8.8.9:9999"), good(9_000_000), t, None)
            .unwrap();
        store
            .record(sa("10.0.0.1:8080"), good(9_000_000), t, None)
            .unwrap();
        store
            .record(
                sa("8.8.8.10:8080"),
                Outcome::Good {
                    version: 70214,
                    services: 1,
                    height: 9_000_000,
                    harvested: false,
                    chain: ChainCheck::NotChecked,
                },
                t,
                None,
            )
            .unwrap();
        assert_eq!(store.tip(&policy(), t).unwrap(), 1000);
    }

    #[test]
    fn one_lying_height_does_not_unserve_honest_nodes() {
        let (_d, store) = open();
        let t = 100;
        for i in 1..=5 {
            store
                .record(sa(&format!("8.8.8.{i}:8080")), good(1000 + i), t, None)
                .unwrap();
        }
        store
            .record(sa("8.8.4.4:8080"), good(10_000_000), t, None)
            .unwrap();
        let got = store.servable(&policy(), t + 10).unwrap();
        assert_eq!(got.len(), 6);
        let honest = got
            .iter()
            .filter(|s| s.addr.ip().to_string().starts_with("8.8.8."))
            .count();
        assert_eq!(honest, 5);
    }

    #[test]
    fn wrong_port_and_port_zero_gossip_is_not_stored() {
        let (_d, store) = open();
        let n = store
            .add_learned(
                &[
                    entry("8.8.8.8", 9999),
                    entry("8.8.8.9", 0),
                    entry("8.8.8.10", 9969),
                ],
                1,
                Some(9969),
            )
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(
            store.due(1, 10, false, None).unwrap(),
            vec![due_at("8.8.8.10:9969", true)]
        );
        // `None` accepts any non-zero port.
        let n = store
            .add_learned(&[entry("8.8.8.8", 9999), entry("8.8.8.9", 0)], 1, None)
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn due_queries_use_partial_indexes_without_sorting() {
        let (_d, store) = open();
        let conn = store.conn();
        for (private, expect) in [(false, "_pub"), (true, "_any")] {
            for kind in [DueKind::GoodRetry, DueKind::New, DueKind::BadRetry] {
                let sql = due_sql(kind, private);
                let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
                let plan: Vec<String> = stmt
                    .query_map(params![5, 5, 5], |r| r.get::<_, String>(3))
                    .unwrap()
                    .map(Result::unwrap)
                    .collect();
                let plan = plan.join(" | ");
                assert!(
                    plan.contains("USING INDEX") || plan.contains("USING COVERING INDEX"),
                    "{plan}"
                );
                assert!(plan.contains(expect), "{plan}");
                assert!(!plan.contains("TEMP B-TREE"), "{plan}");
                println!("{plan}");
            }
        }
    }

    #[test]
    fn due_honours_backoff_and_prefers_good_retries_then_new() {
        let (_d, store) = open();
        let t = 1_000_000;
        store
            .add_learned(&[entry("8.8.8.1", 8080), entry("8.8.8.2", 8080)], t, None)
            .unwrap();
        store.record(sa("8.8.8.1:8080"), good(1), t, None).unwrap();
        let due = store.due(t + 1, 10, false, None).unwrap();
        assert_eq!(due, vec![due_at("8.8.8.2:8080", true)]);
        // Good: retry after 15 min.
        let due = store.due(t + 900, 10, false, None).unwrap();
        assert_eq!(due.len(), 2);
        assert_eq!(due[0].addr, sa("8.8.8.1:8080"));
        assert_eq!(due[1].addr, sa("8.8.8.2:8080"));
        // Bad: 5 min * 2^failures, capped at 24 h.
        let b = sa("8.8.8.3:8080");
        store.record(b, Outcome::Bad, t, None).unwrap();
        assert!(
            !store
                .due(t + 599, 10, false, None)
                .unwrap()
                .iter()
                .any(|d| d.addr == b)
        );
        assert!(
            store
                .due(t + 600, 10, false, None)
                .unwrap()
                .iter()
                .any(|d| d.addr == b)
        );
        for _ in 0..40 {
            store.record(b, Outcome::Bad, t, None).unwrap();
        }
        assert!(
            !store
                .due(t + 86_399, 10, false, None)
                .unwrap()
                .iter()
                .any(|d| d.addr == b)
        );
        assert!(
            store
                .due(t + 86_400, 10, false, None)
                .unwrap()
                .iter()
                .any(|d| d.addr == b)
        );
        assert_eq!(store.due(t + 86_400, 1, false, None).unwrap().len(), 1);
    }

    #[test]
    fn backoff_after_seventy_failures_is_the_24_hour_cap() {
        let (_d, store) = open();
        let t = 1_000_000;
        let b = sa("8.8.8.1:8080");
        for _ in 0..70 {
            store.record(b, Outcome::Bad, t, None).unwrap();
        }
        let is_due = |now: i64| {
            store
                .due(now, 10, false, None)
                .unwrap()
                .iter()
                .any(|d| d.addr == b)
        };
        assert!(!is_due(t + 86_399));
        assert!(is_due(t + 86_400));
    }

    #[test]
    fn due_splits_the_budget_evenly_and_gives_back_unused_share() {
        let (_d, store) = open();
        let t = 1_000_000;
        let new: Vec<AddrEntry> = (0..8)
            .map(|i| entry(&format!("9.9.9.{}", i + 1), 8080))
            .collect();
        store.add_learned(&new, t, None).unwrap();
        let in_prefix = |a: &SocketAddr, p: &str| a.ip().to_string().starts_with(p);
        let count = |due: &[Due]| {
            (
                due.iter().filter(|d| in_prefix(&d.addr, "7.7.7.")).count(),
                due.iter().filter(|d| in_prefix(&d.addr, "9.9.9.")).count(),
            )
        };
        for i in 0..2 {
            store
                .record(sa(&format!("7.7.7.{}:8080", i + 1)), Outcome::Bad, t, None)
                .unwrap();
        }
        // Only 2 bad nodes are due: they take 2, the other 8 go to never-tried.
        let due = store.due(t + 600, 10, false, None).unwrap();
        assert_eq!((due.len(), count(&due)), (10, (2, 8)));
        for i in 2..8 {
            store
                .record(sa(&format!("7.7.7.{}:8080", i + 1)), Outcome::Bad, t, None)
                .unwrap();
        }
        // 8 of each are due: 5 of each.
        let due = store.due(t + 600, 10, false, None).unwrap();
        assert_eq!((due.len(), count(&due)), (10, (5, 5)));
    }

    #[test]
    fn due_serves_good_retries_first_then_splits_new_and_bad() {
        let (_d, store) = open();
        let t = 1_000_000;
        let new: Vec<AddrEntry> = (0..500)
            .map(|i| entry(&format!("9.{}.{}.1", i / 200, i % 200), 8080))
            .collect();
        store.add_learned(&new, t, None).unwrap();
        for i in 0..5 {
            store
                .record(sa(&format!("8.8.8.{}:8080", i + 1)), good(1), t, None)
                .unwrap();
            store
                .record(sa(&format!("7.7.7.{}:8080", i + 1)), Outcome::Bad, t, None)
                .unwrap();
        }
        let now = t + 86_400;
        let due = store.due(now, 20, false, None).unwrap();
        assert_eq!(due.len(), 20);
        let in_prefix = |a: &SocketAddr, p: &str| a.ip().to_string().starts_with(p);
        assert!(due[..5].iter().all(|d| in_prefix(&d.addr, "8.8.8.")));
        let bad = due.iter().filter(|d| in_prefix(&d.addr, "7.7.7.")).count();
        let fresh = due.iter().filter(|d| in_prefix(&d.addr, "9.")).count();
        assert_eq!((bad, fresh), (5, 10));
        // A group that cannot use its share hands it over.
        let due = store.due(now, 30, false, None).unwrap();
        let bad = due.iter().filter(|d| in_prefix(&d.addr, "7.7.7.")).count();
        assert_eq!((due.len(), bad), (30, 5));
        let due = store.due(now, 600, false, None).unwrap();
        assert_eq!(due.len(), 510);
    }

    #[test]
    fn schema_version_is_set_and_newer_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("n.db");
        drop(NodeStore::open(&path).unwrap());
        let c = Connection::open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 2);
        // An unversioned existing database is adopted at the current version.
        c.pragma_update(None, "user_version", 0).unwrap();
        drop(c);
        drop(NodeStore::open(&path).unwrap());
        let c = Connection::open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 2);
        c.pragma_update(None, "user_version", 3).unwrap();
        drop(c);
        let err = NodeStore::open(&path).err().unwrap().to_string();
        assert!(err.contains("schema version 3"), "{err}");
    }

    #[test]
    fn harvest_flag_once_per_day() {
        let (_d, store) = open();
        let a = sa("8.8.8.1:8080");
        let t = 1_000_000;
        store
            .record(
                a,
                Outcome::Good {
                    version: 70216,
                    services: 1,
                    height: 1,
                    harvested: true,
                    chain: ChainCheck::NotChecked,
                },
                t,
                None,
            )
            .unwrap();
        assert!(!store.due(t + 900, 10, false, None).unwrap()[0].harvest);
        store.record(a, good(1), t + 900, None).unwrap();
        assert!(!store.due(t + 86_399, 10, false, None).unwrap()[0].harvest);
        assert!(store.due(t + 86_400, 10, false, None).unwrap()[0].harvest);
    }

    #[test]
    fn learned_unroutable_and_duplicates_not_due() {
        let (_d, store) = open();
        let inserted = store
            .add_learned(
                &[
                    entry("127.0.0.1", 8080),
                    entry("192.168.1.1", 8080),
                    entry("8.8.8.8", 8080),
                    entry("8.8.8.8", 8080),
                    entry("::ffff:10.1.2.3", 8080),
                    entry("::ffff:8.8.8.8", 8080),
                    AddrEntry {
                        addr: NetAddr::TorV3([7; 32]),
                        port: 8080,
                        services: 1,
                        time: 0,
                    },
                ],
                5,
                None,
            )
            .unwrap();
        assert_eq!(inserted, 4);
        let due = store.due(5, 10, false, None).unwrap();
        assert_eq!(due, vec![due_at("8.8.8.8:8080", true)]);
        assert_eq!(store.due(5, 10, true, None).unwrap().len(), 4);
    }

    #[test]
    fn mapped_address_record_hits_the_v4_row() {
        let (_d, store) = open();
        store
            .add_learned(&[entry("8.8.8.8", 8080)], 0, None)
            .unwrap();
        store
            .record(sa("[::ffff:8.8.8.8]:8080"), good(1000), 10, None)
            .unwrap();
        let s = store.servable(&policy(), 20).unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].addr, sa("8.8.8.8:8080"));
    }

    #[test]
    fn routability_classes() {
        for bad in [
            "0.0.0.0",
            "127.0.0.1",
            "10.1.1.1",
            "172.16.0.1",
            "192.168.0.1",
            "198.18.0.1",
            "198.19.255.255",
            "169.254.1.1",
            "192.0.2.1",
            "198.51.100.1",
            "203.0.113.1",
            "100.64.0.1",
            "100.127.255.255",
            "224.0.0.1",
            "::",
            "::1",
            "fc00::1",
            "fd12::1",
            "fe80::1",
            "2001:db8::1",
            "ff02::1",
            "::ffff:192.168.1.1",
        ] {
            assert!(!is_routable(bad.parse().unwrap()), "{bad}");
        }
        for good in [
            "8.8.8.8",
            "100.128.0.1",
            "198.20.0.1",
            "2606:4700::1",
            "::ffff:8.8.8.8",
        ] {
            assert!(is_routable(good.parse().unwrap()), "{good}");
        }
    }

    #[test]
    fn reopen_keeps_servable_set() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nodes.db");
        let t = 9_000;
        // Floor 0 disables warm-up, so only persisted full-bar stats can serve.
        let no_warmup = ServablePolicy {
            warmup_floor: 0,
            ..policy()
        };
        let before = {
            let store = NodeStore::open(&path).unwrap();
            for i in 1..=4 {
                make_full_bar(&store, sa(&format!("8.8.8.{i}:8080")), t);
            }
            store.servable(&no_warmup, t + 1300).unwrap()
        };
        assert_eq!(before.len(), 4);
        let store = NodeStore::open(&path).unwrap();
        assert_eq!(store.servable(&no_warmup, t + 1300).unwrap(), before);
    }

    fn chain_of(store: &NodeStore, addr: &str) -> (i64, String, i64, i64) {
        let a = sa(addr);
        store
            .conn()
            .query_row(
                "SELECT chain, chain_ref, last_ok, next_try FROM nodes WHERE ip = ?1 AND port = ?2",
                params![a.ip().to_string(), a.port()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap()
    }

    #[test]
    fn open_existing_reads_a_current_store_and_refuses_others() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nodes.db");

        assert!(NodeStore::open_existing(&path).is_err());
        assert!(!path.exists(), "a missing store must not be created");

        let store = NodeStore::open(&path).unwrap();
        make_full_bar(&store, sa("8.8.8.8:8080"), 10_000);
        drop(store);
        let before = std::fs::read(&path).unwrap();
        let ro = NodeStore::open_existing(&path).unwrap();
        assert_eq!(addrs(&ro, &policy(), 11_200), [sa("8.8.8.8:8080")]);
        assert!(
            ro.add_learned(&[entry("8.8.4.4", 8080)], 1, Some(8080))
                .is_err()
        );
        drop(ro);
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(!dir.path().join("nodes.db-wal").exists());
        assert!(!dir.path().join("nodes.db-shm").exists());

        Connection::open(&path)
            .unwrap()
            .pragma_update(None, "user_version", 3)
            .unwrap();
        let err = NodeStore::open_existing(&path).err().unwrap().to_string();
        assert!(err.contains("schema 3"), "{err}");
    }

    fn checked(chain: ChainCheck) -> Outcome {
        Outcome::Good {
            version: 70216,
            services: 1,
            height: 1000,
            harvested: false,
            chain,
        }
    }

    #[test]
    fn servable_requires_verified_under_the_current_checkpoint() {
        let (_d, store) = open();
        let t = 100;
        let (a, b, c) = ("8.8.8.1:8080", "8.8.8.2:8080", "8.8.8.3:8080");
        store
            .record(sa(a), checked(ChainCheck::Verified), t, Some("A"))
            .unwrap();
        store
            .record(sa(b), checked(ChainCheck::Verified), t, Some("B"))
            .unwrap();
        store
            .record(sa(c), checked(ChainCheck::NotChecked), t, Some("A"))
            .unwrap();
        let under_a = ServablePolicy {
            checkpoint_ref: Some("A".into()),
            ..policy()
        };
        assert_eq!(addrs(&store, &under_a, t + 10), vec![sa(a)]);
        assert_eq!(addrs(&store, &policy(), t + 10), vec![sa(a), sa(b), sa(c)]);
        // A later probe that did not check (or could not) keeps the stored verdict.
        store
            .record(sa(a), checked(ChainCheck::NotChecked), t + 900, Some("A"))
            .unwrap();
        store
            .record(sa(a), checked(ChainCheck::Unverified), t + 1800, Some("A"))
            .unwrap();
        assert_eq!(chain_of(&store, a).0, 1);
        assert_eq!(addrs(&store, &under_a, t + 1810), vec![sa(a)]);
    }

    #[test]
    fn wrong_chain_is_a_failed_probe_and_not_servable() {
        let (_d, store) = open();
        let t = 10_000;
        let a = "8.8.8.1:8080";
        store
            .record(sa(a), checked(ChainCheck::Verified), t, Some("A"))
            .unwrap();
        store
            .record(
                sa(a),
                Outcome::WrongChain { harvested: false },
                t + 900,
                Some("A"),
            )
            .unwrap();
        assert_eq!(chain_of(&store, a), (2, "A".into(), 0, t + 900 + 600));
        let under_a = ServablePolicy {
            checkpoint_ref: Some("A".into()),
            ..policy()
        };
        assert!(addrs(&store, &under_a, t + 910).is_empty());
        assert!(addrs(&store, &policy(), t + 910).is_empty());
        assert!(
            !store
                .due(t + 900 + 599, 10, false, Some("A"))
                .unwrap()
                .iter()
                .any(|d| d.addr == sa(a))
        );
        store
            .record(
                sa(a),
                Outcome::WrongChain { harvested: true },
                t + 1500,
                Some("A"),
            )
            .unwrap();
        assert_eq!(chain_of(&store, a).3, t + 1500 + 1200);
        // The harvest is recorded, so the next back-off re-probe does not ask again.
        let due = store.due(t + 2700, 10, false, Some("A")).unwrap();
        assert_eq!(
            due,
            vec![Due {
                addr: sa(a),
                harvest: false,
                check_chain: true
            }]
        );
    }

    #[test]
    fn due_flags_chain_checks() {
        let (_d, store) = open();
        let t = 1_000_000;
        let a = sa("8.8.8.1:8080");
        store
            .record(
                a,
                Outcome::Good {
                    version: 70216,
                    services: 1,
                    height: 1000,
                    harvested: true,
                    chain: ChainCheck::Verified,
                },
                t,
                Some("A"),
            )
            .unwrap();
        let fresh = sa("8.8.8.2:8080");
        store
            .add_learned(&[entry("8.8.8.2", 8080)], t, None)
            .unwrap();
        let due = store.due(t + 900, 10, false, Some("A")).unwrap();
        assert_eq!(
            due,
            vec![
                Due {
                    addr: a,
                    harvest: false,
                    check_chain: false
                },
                Due {
                    addr: fresh,
                    harvest: true,
                    check_chain: true
                },
            ]
        );
        // Verified under an old checkpoint counts as unknown.
        let due = store.due(t + 900, 10, false, Some("B")).unwrap();
        assert!(due[0].addr == a && !due[0].harvest && due[0].check_chain);
        // A harvest probe always checks.
        let due = store.due(t + 86_400, 10, false, Some("A")).unwrap();
        assert!(due[0].addr == a && due[0].harvest && due[0].check_chain);
    }
}
