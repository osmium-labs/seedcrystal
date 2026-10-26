//! Seedcrystal, a multi-coin DNS seeder for Bitcoin-protocol UTXO chains.
//!
//! - [`coin`]: coin presets, the per-network parameters, built from `presets/*.toml`.
//! - [`config`]: the TOML config file.
//! - [`p2p`]: the wire format and the one-shot peer probe.
//! - [`crawler`]: probes nodes, records the results and publishes servable snapshots.
//! - [`store`]: the SQLite crawl database and the servable-node policy.
//! - [`score`]: reliability scoring over decaying time windows.
//! - [`dns`]: the authoritative DNS server, answer selection and the query log.
//! - [`cloudflare`]: Cloudflare mode, publishing snapshots as DNS records.
//! - [`doctor`]: checks a seed from the outside, the way a node resolves it.
//! - [`seeds`]: the fixed-seed list for a coin's `generate-seeds.py`.

pub mod cloudflare;
pub mod coin;
pub mod config;
pub mod crawler;
pub mod dns;
pub mod doctor;
pub mod p2p;
pub mod score;
pub mod seeds;
pub mod store;
