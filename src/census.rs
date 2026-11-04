//! What the crawled network runs: the nodes that answered a handshake in the last 24 hours,
//! counted by user agent and by protocol version. The crawler writes it to
//! `<data_dir>/census-<preset id>.json` once a minute; `doctor` reads it.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub use crate::dns::querylog::OTHER;
use crate::store::CensusRow;

/// How far back a handshake counts.
pub const WINDOW_S: i64 = 24 * 3600;
/// Agent rows kept; the rest are summed into [`OTHER`].
pub const MAX_AGENTS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Census {
    /// Unix seconds when it was counted.
    pub built_at: i64,
    pub window_s: i64,
    /// Peers below this protocol version are refused at the handshake, so never counted.
    pub min_protocol: i32,
    /// The network tip, as served.
    pub tip: i32,
    pub nodes: u64,
    /// Nodes among `nodes` whose chain was never verified against the checkpoint; `None` when
    /// the preset has no checkpoint, so no chain is ever checked.
    pub unverified: Option<u64>,
    /// Most nodes first; [`OTHER`] last.
    pub agents: Vec<AgentCount>,
    /// Highest version first.
    pub protocol_versions: Vec<ProtocolCount>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCount {
    pub user_agent: String,
    pub nodes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolCount {
    pub version: i32,
    pub nodes: u64,
}

/// A peer-supplied agent without control characters (lossy decoding can yield C1 controls).
pub fn clean_agent(agent: &str) -> String {
    agent.chars().filter(|c| !c.is_control()).collect()
}

/// `chain_checked`: the preset has a checkpoint, so nodes' chains are checked against it.
pub fn build(
    rows: &[CensusRow],
    built_at: i64,
    tip: i32,
    min_protocol: i32,
    chain_checked: bool,
) -> Census {
    let mut by_agent: HashMap<String, u64> = HashMap::new();
    let mut by_version: BTreeMap<i32, u64> = BTreeMap::new();
    // A peer calling itself `_other` joins the overflow row rather than making a second one.
    let mut other = 0;
    for r in rows {
        let agent = clean_agent(&r.user_agent);
        if agent == OTHER {
            other += 1;
        } else {
            *by_agent.entry(agent).or_default() += 1;
        }
        *by_version.entry(r.version).or_default() += 1;
    }
    let mut agents: Vec<AgentCount> = by_agent
        .into_iter()
        .map(|(user_agent, nodes)| AgentCount { user_agent, nodes })
        .collect();
    agents
        .sort_by(|a, b| (Reverse(a.nodes), &a.user_agent).cmp(&(Reverse(b.nodes), &b.user_agent)));
    if agents.len() > MAX_AGENTS {
        other += agents.drain(MAX_AGENTS..).map(|a| a.nodes).sum::<u64>();
    }
    if other > 0 {
        agents.push(AgentCount {
            user_agent: OTHER.to_string(),
            nodes: other,
        });
    }
    Census {
        built_at,
        window_s: WINDOW_S,
        min_protocol,
        tip,
        nodes: rows.len() as u64,
        unverified: chain_checked.then(|| rows.iter().filter(|r| r.unverified).count() as u64),
        agents,
        protocol_versions: by_version
            .into_iter()
            .rev()
            .map(|(version, nodes)| ProtocolCount { version, nodes })
            .collect(),
    }
}

/// The file for one network.
pub fn path_for(data_dir: &Path, preset_id: &str) -> PathBuf {
    data_dir.join(format!("census-{preset_id}.json"))
}

/// Reads a census written by the seeder.
pub fn read(path: &Path) -> Result<Census> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("read census {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse census {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(agent: &str, version: i32) -> CensusRow {
        CensusRow {
            user_agent: agent.into(),
            version,
            unverified: false,
        }
    }

    fn rows(spec: &[(&str, usize)]) -> Vec<CensusRow> {
        spec.iter()
            .flat_map(|&(a, n)| std::iter::repeat_with(move || row(a, 70231)).take(n))
            .collect()
    }

    fn agents(c: &Census) -> Vec<(&str, u64)> {
        c.agents
            .iter()
            .map(|a| (a.user_agent.as_str(), a.nodes))
            .collect()
    }

    #[test]
    fn agents_sort_by_count_then_name_and_versions_descend() {
        let mut r = rows(&[("/b/", 2), ("/a/", 2), ("/c/", 5), ("", 1)]);
        r[0].version = 70215;
        let c = build(&r, 1, 2, 3, true);
        assert_eq!(agents(&c), [("/c/", 5), ("/a/", 2), ("/b/", 2), ("", 1)]);
        assert_eq!(
            c.protocol_versions,
            [
                ProtocolCount {
                    version: 70231,
                    nodes: 9
                },
                ProtocolCount {
                    version: 70215,
                    nodes: 1
                }
            ]
        );
    }

    #[test]
    fn sixty_five_agents_become_sixty_four_and_other() {
        let names: Vec<String> = (0..65).map(|i| format!("/n{i:02}/")).collect();
        let mut r: Vec<CensusRow> = names.iter().map(|n| row(n, 70231)).collect();
        r.extend(rows(&[("/big/", 3)]));
        let c = build(&r, 1, 2, 3, true);
        assert_eq!(c.agents.len(), 65);
        assert_eq!(c.agents[0].user_agent, "/big/");
        // Ties sort by name, so the last two of /n00/../n64/ are folded.
        assert_eq!(c.agents[63].user_agent, "/n62/");
        assert_eq!(
            c.agents[64],
            AgentCount {
                user_agent: OTHER.into(),
                nodes: 2
            }
        );
        assert_eq!(c.agents.iter().map(|a| a.nodes).sum::<u64>(), 68);
    }

    #[test]
    fn a_peer_named_other_folds_into_the_one_other_row() {
        let c = build(
            &rows(&[("/a/", 3), ("_other", 2), ("/b/", 1)]),
            1,
            2,
            3,
            true,
        );
        assert_eq!(agents(&c), [("/a/", 3), ("/b/", 1), (OTHER, 2)]);
    }

    #[test]
    fn control_characters_are_stripped_before_grouping() {
        let c = build(
            &rows(&[("/x:1/", 1), ("/x:1/\u{85}", 1), ("\u{7}/x:1/", 1)]),
            1,
            2,
            3,
            true,
        );
        assert_eq!(agents(&c), [("/x:1/", 3)]);
    }

    #[test]
    fn totals_agree() {
        let mut r = rows(&[("/a/", 4), ("/b/", 3), ("", 2)]);
        r[1].unverified = true;
        r[5].unverified = true;
        r[2].version = 70220;
        r[6].version = 70215;
        let c = build(&r, 1, 2, 3, true);
        assert_eq!(c.nodes, 9);
        assert_eq!(c.unverified, Some(2));
        assert_eq!(c.agents.iter().map(|a| a.nodes).sum::<u64>(), c.nodes);
        assert_eq!(
            c.protocol_versions.iter().map(|p| p.nodes).sum::<u64>(),
            c.nodes
        );
    }

    #[test]
    fn json_shape_matches_the_spec() {
        let mut r = rows(&[("/Osmium Core:1.2.0/", 2), ("", 1)]);
        r[0].unverified = true;
        let v = serde_json::to_value(build(&r, 1793714000, 856203, 70215, true)).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "built_at": 1793714000,
                "window_s": 86400,
                "min_protocol": 70215,
                "tip": 856203,
                "nodes": 3,
                "unverified": 1,
                "agents": [
                    { "user_agent": "/Osmium Core:1.2.0/", "nodes": 2 },
                    { "user_agent": "", "nodes": 1 }
                ],
                "protocol_versions": [{ "version": 70231, "nodes": 3 }]
            })
        );
    }

    #[test]
    fn unverified_is_null_without_a_checkpoint() {
        let mut r = rows(&[("/a/", 2)]);
        r[0].unverified = true;
        let c = build(&r, 1, 2, 3, false);
        assert_eq!(c.unverified, None);
        assert_eq!(c.nodes, 2);
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["unverified"], serde_json::Value::Null);
        // The file reads back as written.
        let d = tempfile::tempdir().unwrap();
        let p = path_for(d.path(), "osmium-mainnet");
        crate::jsonfile::write_atomic(&p, &c).unwrap();
        assert_eq!(read(&p).unwrap(), c);
    }

    #[test]
    fn read_reports_a_missing_or_broken_file() {
        let d = tempfile::tempdir().unwrap();
        let p = path_for(d.path(), "osmium-mainnet");
        assert!(p.ends_with("census-osmium-mainnet.json"));
        assert!(format!("{:#}", read(&p).unwrap_err()).contains("census-osmium-mainnet.json"));
        std::fs::write(&p, "{").unwrap();
        assert!(format!("{:#}", read(&p).unwrap_err()).contains("census-osmium-mainnet.json"));
    }
}
