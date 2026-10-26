use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use seedcrystal::cloudflare::api::normalize_name;
use seedcrystal::cloudflare::{self, publish::PublishConfig};
use seedcrystal::coin::{self, CoinPreset};
use seedcrystal::config::{self, CloudflareConfig, Config, NetworkConfig, Publish};
use seedcrystal::crawler::{self, CrawlerConfig, SnapshotRx};
use seedcrystal::dns::answer::published_names;
use seedcrystal::dns::querylog::{self, Counters, QueryLogs};
use seedcrystal::dns::{self, Zone, ZoneSet};
use seedcrystal::doctor::{self, DoctorOptions, Status};
use seedcrystal::seeds::format_seeds;
use seedcrystal::store::NodeStore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "seedcrystal",
    version,
    about = "Multi-coin DNS seeder for Bitcoin-protocol UTXO chains"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Crawl the configured networks and serve them over DNS.
    Run {
        /// Path to the TOML config file.
        #[arg(long, short)]
        config: PathBuf,
    },
    /// Check a seed from the outside, the way a coin node resolves it. Exits 1 if any check fails.
    Doctor {
        /// Path to the TOML config file.
        #[arg(long, short)]
        config: PathBuf,
        /// A recursive resolver to ask, `ip` or `ip:port`; repeatable. Default: 1.1.1.1,
        /// 8.8.8.8 and 9.9.9.9.
        #[arg(long, value_parser = parse_resolver)]
        resolver: Vec<SocketAddr>,
    },
    /// Print good nodes as `ip:port` lines, the input of a coin's `contrib/seeds/generate-seeds.py`.
    Seeds {
        /// Path to the TOML config file.
        #[arg(long, short)]
        config: PathBuf,
        /// The network's seed name; required when the config has several networks.
        #[arg(long)]
        network: Option<String>,
        /// The most lines to print.
        #[arg(long, default_value_t = 50)]
        max: usize,
    },
    /// List the built-in coin presets with their P2P port, seed name prefix and protocol version.
    Presets,
}

fn parse_resolver(s: &str) -> Result<SocketAddr, String> {
    if let Ok(addr) = s.parse::<SocketAddr>() {
        return Ok(addr);
    }
    s.parse::<IpAddr>()
        .map(|ip| SocketAddr::new(ip, 53))
        .map_err(|_| format!("`{s}` is not an IP address or ip:port"))
}

/// Colour only on a terminal, and never when `NO_COLOR` is set to a non-empty value.
/// One line per preset: id, P2P port, the `x<hex>` name its nodes query, protocol version.
fn presets_listing(presets: &[CoinPreset]) -> String {
    let id_width = presets.iter().map(|p| p.id.len()).max().unwrap_or(0);
    let names: Vec<String> = presets
        .iter()
        .map(|p| format!("x{:x}", p.seed_flags))
        .collect();
    let name_width = names.iter().map(String::len).max().unwrap_or(0);
    presets
        .iter()
        .zip(&names)
        .map(|(p, name)| {
            format!(
                "{:<id_width$}  port {:<5}  {:<name_width$}  protocol {}\n",
                p.id, p.default_port, name, p.protocol_version
            )
        })
        .collect()
}

fn use_ansi(is_terminal: bool, no_color: Option<&std::ffi::OsStr>) -> bool {
    is_terminal && no_color.is_none_or(|v| v.is_empty())
}

fn init_tracing() {
    use std::io::IsTerminal;
    let no_color = std::env::var_os("NO_COLOR");
    tracing_subscriber::fmt()
        .with_ansi(use_ansi(
            std::io::stderr().is_terminal(),
            no_color.as_deref(),
        ))
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}

/// Prints every network's checks; true when none failed.
async fn doctor(config_path: &std::path::Path, resolvers: Vec<SocketAddr>) -> Result<bool> {
    let Config { network, .. } = config::load(config_path)?;
    let mut opts = DoctorOptions::default();
    if !resolvers.is_empty() {
        opts.resolvers = resolvers;
    }
    let mut healthy = true;
    for n in &network {
        let preset = n
            .load_preset()
            .with_context(|| format!("network `{}`", n.seed))?;
        println!("{} ({})", n.seed, preset.id);
        for c in doctor::run_checks(n, &preset, &opts).await {
            println!("{c}");
            healthy &= c.status != Status::Fail;
        }
    }
    Ok(healthy)
}

/// The crawl database of a network; `run` and `seeds` must agree on it.
fn store_path(n: &NetworkConfig, preset: &CoinPreset) -> PathBuf {
    n.data_dir.join(format!("{}.sqlite", preset.id))
}

/// The servable nodes of one network as `generate-seeds.py` input.
fn seeds(config_path: &std::path::Path, network: Option<&str>, max: usize) -> Result<String> {
    let Config { network: all, .. } = config::load(config_path)?;
    let n = match (network, all.as_slice()) {
        (Some(name), _) => all
            .iter()
            .find(|n| n.seed == name)
            .with_context(|| format!("no network `{name}` in the config"))?,
        (None, [only]) => only,
        (None, _) => {
            let names: Vec<_> = all.iter().map(|n| n.seed.as_str()).collect();
            anyhow::bail!(
                "the config has several networks; choose one with --network ({})",
                names.join(", ")
            )
        }
    };
    let preset = n
        .load_preset()
        .with_context(|| format!("network `{}`", n.seed))?;
    let path = store_path(n, &preset);
    anyhow::ensure!(
        path.exists(),
        "no crawl database at {}; has `seedcrystal run` crawled this network yet?",
        path.display()
    );
    let store = NodeStore::open_existing(&path)?;
    let policy = crawler::policy_for(&preset, n.allow_private);
    let now = i64::try_from(querylog::unix_now()).unwrap_or(i64::MAX);
    let nodes = store.servable(&policy, now)?;
    Ok(format_seeds(&nodes, max))
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Presets => {
            match coin::builtin_ids()
                .map(coin::builtin)
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(presets) => {
                    print!("{}", presets_listing(&presets));
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Seeds {
            config,
            network,
            max,
        } => {
            init_tracing();
            match seeds(&config, network.as_deref(), max) {
                Ok(text) => {
                    use std::io::Write;
                    match std::io::stdout().lock().write_all(text.as_bytes()) {
                        Ok(()) => ExitCode::SUCCESS,
                        Err(e) => {
                            eprintln!("error: cannot write to stdout: {e}");
                            ExitCode::FAILURE
                        }
                    }
                }
                Err(e) => {
                    eprintln!("error: {e:#}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Doctor { config, resolver } => {
            init_tracing();
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    error!("cannot start the async runtime: {e}");
                    return ExitCode::FAILURE;
                }
            };
            match rt.block_on(doctor(&config, resolver)) {
                Ok(true) => ExitCode::SUCCESS,
                Ok(false) => ExitCode::FAILURE,
                Err(e) => {
                    error!("{e:#}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Run { config } => {
            init_tracing();
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    error!("cannot start the async runtime: {e}");
                    return ExitCode::FAILURE;
                }
            };
            match rt.block_on(run(&config)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    error!("{}", explain(&e));
                    ExitCode::FAILURE
                }
            }
        }
    }
}

/// The error chain on one line, with a hint for the usual bind failures.
fn explain(e: &anyhow::Error) -> String {
    let mut text = format!("{e:#}");
    if !text.contains("bind ") {
        return text;
    }
    for cause in e.chain() {
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            match io.kind() {
                std::io::ErrorKind::AddrInUse => {
                    text.push_str(" (address already in use: is another DNS server on that port?)")
                }
                std::io::ErrorKind::PermissionDenied => text.push_str(
                    " (permission denied: ports below 1024 need root or \
                     `setcap 'cap_net_bind_service=+ep'` on the binary)",
                ),
                _ => continue,
            }
            break;
        }
    }
    text
}

type Tasks = Vec<(String, JoinHandle<Result<()>>)>;

/// A network after its preset, data dir and (for Cloudflare) token have been resolved.
struct Prepared {
    preset: CoinPreset,
    publisher: Option<(cloudflare::api::Client, PublishConfig)>,
}

/// Resolves every network's preset, data dir and Cloudflare client before anything is
/// spawned, so a bad second network cannot leave the first one half started.
fn prepare(network: &[NetworkConfig]) -> Result<Vec<Prepared>> {
    let token_env = std::env::var(config::TOKEN_ENV).ok();
    network
        .iter()
        .map(|n| {
            let preset = n
                .load_preset()
                .with_context(|| format!("network `{}`", n.seed))?;
            std::fs::create_dir_all(&n.data_dir)
                .with_context(|| format!("create data_dir {}", n.data_dir.display()))?;
            let publisher = match (&n.cloudflare, n.publish) {
                (Some(cf), Publish::Cloudflare) => Some(
                    prepare_publisher(n, cf, &preset, token_env.as_deref())
                        .with_context(|| format!("network `{}`", n.seed))?,
                ),
                _ => None,
            };
            Ok(Prepared { preset, publisher })
        })
        .collect()
}

fn prepare_publisher(
    n: &NetworkConfig,
    cf: &CloudflareConfig,
    preset: &CoinPreset,
    token_env: Option<&str>,
) -> Result<(cloudflare::api::Client, PublishConfig)> {
    let token = config::resolve_token(cf, token_env)?;
    let client = cloudflare::api::Client::new(&cf.api_base, &cf.zone_id, token)?;
    let mut pc = PublishConfig::new(
        config::fqdn(&n.seed)?,
        preset.filters.clone(),
        preset.seed_flags,
    );
    pc.ttl = cf.ttl;
    pc.interval = Duration::from_secs(cf.interval_secs);
    pc.per_name = cf.per_name;
    pc.dry_run = cf.dry_run;
    pc.state_path = Some(n.data_dir.join(format!("shrink-guard-{}.json", preset.id)));
    pc.validate()?;
    Ok((client, pc))
}

/// Starts one crawler per network and returns each one's snapshot, in network order.
fn start_crawlers(
    network: &[NetworkConfig],
    prepared: &[Prepared],
    shutdown: &CancellationToken,
    crawlers: &mut Tasks,
) -> Result<Vec<SnapshotRx>> {
    let mut snapshots = Vec::new();
    for (n, p) in network.iter().zip(prepared) {
        let preset = &p.preset;
        let store = store_path(n, preset);
        let mut cc = CrawlerConfig::new(preset.clone(), store.clone());
        cc.bootstrap = n.bootstrap.clone();
        cc.allow_private = n.allow_private;
        let (snapshot, handle) = crawler::spawn(cc, shutdown.clone())
            .with_context(|| format!("start crawler for `{}`", n.seed))?;
        crawlers.push((n.seed.clone(), handle));
        info!(
            seed = %n.seed, publish = ?n.publish, preset = %preset.id,
            store = %store.display(), "network configured"
        );
        snapshots.push(snapshot);
    }
    Ok(snapshots)
}

/// The authoritative networks' zones, their query counters, and where each is written.
struct Authoritative {
    zones: Vec<Zone>,
    logs: QueryLogs,
    files: Vec<(Arc<Counters>, PathBuf)>,
}

fn authoritative_zones(
    network: &[NetworkConfig],
    prepared: &[Prepared],
    snapshots: &[SnapshotRx],
) -> Result<Authoritative> {
    let mut zones = Vec::new();
    let mut logs = QueryLogs::new();
    let mut files = Vec::new();
    for ((n, p), snapshot) in network.iter().zip(prepared).zip(snapshots) {
        let (Publish::Authoritative, Some(ns), Some(mailbox)) =
            (n.publish, &n.nameserver, &n.mailbox)
        else {
            continue;
        };
        let apex = config::fqdn(&n.seed)?;
        info!(nameserver = %ns, mailbox = %mailbox, seed = %n.seed, "zone configured");
        let published = published_names(&apex, &p.preset.filters)
            .into_iter()
            .map(|(name, _)| normalize_name(&name.to_ascii()));
        let counters = Arc::new(Counters::with_known(querylog::unix_now(), published));
        logs.add(apex.clone(), counters.clone());
        files.push((counters, querylog::path_for(&n.data_dir, &p.preset.id)));
        zones.push(Zone {
            apex,
            nameserver: config::fqdn(ns)?,
            mailbox: config::fqdn(mailbox)?,
            seed_flags: p.preset.seed_flags,
            snapshot: snapshot.clone(),
        });
    }
    Ok(Authoritative { zones, logs, files })
}

async fn stop_all(shutdown: &CancellationToken, tasks: Tasks) {
    shutdown.cancel();
    for (_, h) in tasks {
        let _ = h.await;
    }
}

async fn run(config_path: &std::path::Path) -> Result<()> {
    let Config { dns, network } = config::load(config_path)?;
    let prepared = prepare(&network)?;
    let any_authoritative = network.iter().any(|n| n.publish == Publish::Authoritative);
    let bound = if any_authoritative {
        Some(dns::bind(&dns.listen)?)
    } else {
        None
    };
    let shutdown = CancellationToken::new();
    let mut crawlers: Tasks = Vec::new();

    let snapshots = match start_crawlers(&network, &prepared, &shutdown, &mut crawlers) {
        Ok(s) => s,
        Err(e) => {
            // Stop the crawlers that already started and let them finish their writes.
            stop_all(&shutdown, crawlers).await;
            return Err(e);
        }
    };
    let Authoritative { zones, logs, files } =
        match authoritative_zones(&network, &prepared, &snapshots) {
            Ok(v) => v,
            Err(e) => {
                stop_all(&shutdown, crawlers).await;
                return Err(e);
            }
        };

    let mut publishers: Tasks = Vec::new();
    for ((n, p), snapshot) in network.iter().zip(prepared).zip(snapshots) {
        if let Some((client, pc)) = p.publisher {
            info!(
                seed = %n.seed, dry_run = pc.dry_run, interval_secs = pc.interval.as_secs(),
                "publishing to Cloudflare"
            );
            publishers.push((
                n.seed.clone(),
                cloudflare::publish::spawn(client, pc, snapshot, shutdown.clone()),
            ));
        }
    }

    let mut server = bound.map(|bound| {
        for addr in &dns.listen {
            info!(%addr, "dns listener (udp+tcp)");
        }
        tokio::spawn(bound.run_logged(
            Arc::new(ZoneSet::new(zones)),
            Some(Arc::new(logs)),
            shutdown.clone(),
        ))
    });
    let writers: Vec<_> = files
        .into_iter()
        .map(|(counters, path)| {
            querylog::spawn_writer(counters, path, querylog::WRITE_INTERVAL, shutdown.clone())
        })
        .collect();

    let mut outcome: Result<()> = Ok(());
    let mut server_done = false;
    tokio::select! {
        _ = wait_for_signal() => info!("shutdown signal received"),
        res = async { server.as_mut().expect("guarded").await }, if server.is_some() => {
            server_done = true;
            outcome = flatten(res)
                .and(Err(anyhow::anyhow!("server exited before shutdown")))
                .context("dns server stopped");
        }
        (seed, res) = first_finished(&mut crawlers) => {
            outcome = flatten(res).and(Err(anyhow::anyhow!("crawler exited unexpectedly")))
                .with_context(|| format!("crawler for `{seed}`"));
        }
        (seed, res) = first_finished(&mut publishers) => {
            outcome = flatten(res)
                .and(Err(anyhow::anyhow!("publisher exited before shutdown")))
                .with_context(|| format!("Cloudflare publisher for `{seed}`"));
        }
    }

    shutdown.cancel();
    // A publisher may be mid-cycle; it finishes its HTTP calls rather than being cut off.
    // A second signal is the operator's way out.
    let drain = async {
        if !server_done && let Some(server) = server {
            let r = flatten(server.await).context("dns server");
            if outcome.is_ok() {
                outcome = r;
            }
        }
        for w in writers {
            let _ = w.await;
        }
        for (seed, h) in crawlers {
            let r = flatten(h.await).with_context(|| format!("crawler for `{seed}`"));
            if outcome.is_ok() {
                outcome = r;
            }
        }
        for (seed, h) in publishers {
            let r = flatten(h.await).with_context(|| format!("Cloudflare publisher for `{seed}`"));
            if outcome.is_ok() {
                outcome = r;
            }
        }
        outcome
    };
    tokio::select! {
        outcome = drain => {
            info!("stopped");
            outcome
        }
        _ = wait_for_signal() => {
            error!("second signal during shutdown; exiting immediately");
            std::process::exit(1)
        }
    }
}

fn flatten(res: Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    res.context("task panicked or was aborted")?
}

/// Resolves when any crawler task ends; that task is removed from the list.
async fn first_finished(
    crawlers: &mut Tasks,
) -> (String, Result<Result<()>, tokio::task::JoinError>) {
    if crawlers.is_empty() {
        return std::future::pending().await;
    }
    let (idx, res) = std::future::poll_fn(|cx| {
        for (i, (_, h)) in crawlers.iter_mut().enumerate() {
            if let std::task::Poll::Ready(v) = std::pin::Pin::new(h).poll(cx) {
                return std::task::Poll::Ready((i, v));
            }
        }
        std::task::Poll::Pending
    })
    .await;
    let (seed, _) = crawlers.remove(idx);
    (seed, res)
}

async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

#[cfg(test)]
mod tests {
    use super::{presets_listing, use_ansi};
    use seedcrystal::coin::from_toml;
    use std::ffi::OsStr;

    fn preset(id: &str, port: u16, flags: &str, version: i32) -> seedcrystal::coin::CoinPreset {
        from_toml(&format!(
            "id = \"{id}\"\nmagic = \"a1b2c3d4\"\ndefault_port = {port}\n\
             protocol_version = {version}\nmin_peer_version = 70001\nversion_trailer = \"none\"\n\
             block_interval_secs = 600\nseed_flags = {flags}\nfilters = [{flags}]\n\
             bootstrap_dns = []\nbootstrap_fixed = []\n"
        ))
        .unwrap()
    }

    #[test]
    fn presets_listing_aligns_id_port_seed_name_and_protocol() {
        let listing = presets_listing(&[
            preset("short", 8333, "0x9", 70017),
            preset("a-longer-id", 10226, "0x449", 70220),
        ]);
        assert_eq!(
            listing,
            "short        port 8333   x9    protocol 70017\n\
             a-longer-id  port 10226  x449  protocol 70220\n"
        );
    }

    #[test]
    fn presets_listing_of_nothing_is_empty() {
        assert_eq!(presets_listing(&[]), "");
    }

    #[test]
    fn colour_needs_a_terminal_and_no_no_color() {
        assert!(use_ansi(true, None));
        assert!(use_ansi(true, Some(OsStr::new(""))));
        assert!(!use_ansi(true, Some(OsStr::new("1"))));
        assert!(!use_ansi(false, None));
        assert!(!use_ansi(false, Some(OsStr::new(""))));
    }
}
