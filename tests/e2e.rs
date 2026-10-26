//! Release gate: a real, unmodified node with a fresh datadir takes its first peers from
//! seedcrystal's DNS answers. Needs Docker; run by hand before a release (tests/e2e/README.md):
//!
//! ```sh
//! cargo test --test e2e -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! Each coin is one compose file plus one seeder config under `tests/e2e/`, described by a
//! [`Gate`]; adding a coin means adding those two files and a `#[test]` that calls [`run`].

use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Whole-test cap, image builds included. The watchdog tears down and fails the test at this point.
const HARD_CAP: Duration = Duration::from_secs(8 * 60);
/// Room left under the cap for dumping logs and tearing down.
const TEARDOWN_MARGIN: Duration = Duration::from_secs(60);
/// How long the client may take, after `up`, to report DNS-seeded peers.
const POLL_LIMIT: Duration = Duration::from_secs(6 * 60);
const POLL_EVERY: Duration = Duration::from_secs(5);
const EXEC_LIMIT: Duration = Duration::from_secs(20);
/// Per command of the log dump, which runs inside the teardown margin.
const DUMP_LIMIT: Duration = Duration::from_secs(10);
const DOWN_LIMIT: Duration = Duration::from_secs(120);

/// One coin's end-to-end setup.
struct Gate {
    /// Short coin name, used in the compose project name.
    coin: &'static str,
    /// Compose file, relative to the crate root.
    compose_file: &'static str,
    /// The service whose seeder answers are checked (has `dig`).
    seeder_service: &'static str,
    /// The fresh node that must seed itself from DNS.
    client_service: &'static str,
    /// The client's debug log inside its container.
    client_log: &'static str,
    /// The client's RPC command line, without the method.
    client_cli: &'static [&'static str],
    /// The name the client queries, asked of the seeder for the log dump.
    seed_query: &'static str,
    /// The RPC call whose output must show the client holding a node address.
    probe: &'static [&'static str],
    /// What follows a node's IP in that output: `"` after a bare address, `:` before a port.
    probe_after_ip: &'static str,
    /// The network's node addresses; the client must learn at least one of them.
    nodes: &'static [&'static str],
}

const OSMIUM: Gate = Gate {
    coin: "osmium",
    compose_file: "tests/e2e/docker-compose.yml",
    seeder_service: "seeder",
    client_service: "client",
    client_log: "/data/debug.log",
    client_cli: &["osmium-cli", "-testnet", "-datadir=/data"],
    seed_query: "x1.testnet-seed.osmium.space",
    probe: &["getnodeaddresses", "0"],
    probe_after_ip: "\"",
    nodes: &["11.99.0.11", "11.99.0.12", "11.99.0.13"],
};

#[test]
#[ignore = "needs Docker and about 2 GB of images; run by hand before a release"]
fn osmium_node_seeds_from_seedcrystal() {
    run(&OSMIUM);
}

const RAPTOREUM: Gate = Gate {
    coin: "raptoreum",
    compose_file: "tests/e2e/raptoreum-compose.yml",
    seeder_service: "seeder",
    client_service: "client",
    client_log: "/data/debug.log",
    client_cli: &["raptoreum-cli", "-testnet", "-datadir=/data"],
    seed_query: "x1.lbtn.raptoreum.com",
    // Raptoreum's `getnodeaddresses` cannot show this: its CLI sends the count as a string
    // (no conversion entry), it rejects 0, and it returns at most 23% of the address
    // manager, which is nothing below five entries. The client has no addnode, the testnet
    // fixed seeds are empty (src/chainparamsseeds.h at the tag) and the network is internal,
    // so a `getpeerinfo` entry at a node's address is a peer taken from DNS.
    probe: &["getpeerinfo"],
    probe_after_ip: ":",
    nodes: &["11.99.0.21", "11.99.0.22", "11.99.0.23"],
};

#[test]
#[ignore = "needs Docker and about 2 GB of images; run by hand before a release"]
fn raptoreum_node_seeds_from_seedcrystal() {
    run(&RAPTOREUM);
}

struct Output {
    ok: bool,
    stdout: String,
    stderr: String,
}

/// Runs `cmd` to completion or until `deadline`, when it is killed.
fn bounded(mut cmd: Command, deadline: Instant) -> Output {
    let shown = format!("{cmd:?}");
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {shown}: {e}"));
    let drain = |mut pipe: Box<dyn Read + Send>| {
        thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = pipe.read_to_end(&mut buf);
            String::from_utf8_lossy(&buf).into_owned()
        })
    };
    let out = drain(Box::new(child.stdout.take().unwrap()));
    let err = drain(Box::new(child.stderr.take().unwrap()));
    let status = loop {
        if let Some(status) = child.try_wait().expect("wait for child") {
            break Some(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        thread::sleep(Duration::from_millis(100));
    };
    let mut stderr = err.join().unwrap();
    if status.is_none() {
        stderr.push_str(&format!("\n[killed at its time limit: {shown}]"));
    }
    Output {
        ok: status.is_some_and(|s| s.success()),
        stdout: out.join().unwrap(),
        stderr,
    }
}

#[derive(Clone)]
struct Compose {
    project: String,
    file: PathBuf,
    /// Test steps stop here, leaving the teardown margin before the watchdog fires.
    cap: Instant,
}

fn exec_args<'a>(service: &'a str, argv: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec!["exec", "-T", service];
    args.extend_from_slice(argv);
    args
}

impl Compose {
    /// Runs a compose command bounded by `limit` only; for the dump and teardown.
    fn run_uncapped(&self, args: &[&str], limit: Duration) -> Output {
        let mut cmd = Command::new("docker");
        cmd.arg("compose")
            .arg("-p")
            .arg(&self.project)
            .arg("-f")
            .arg(&self.file)
            .args(args);
        bounded(cmd, Instant::now() + limit)
    }

    /// Runs a test step, bounded by `limit` and by the cap.
    fn run(&self, args: &[&str], limit: Duration) -> Output {
        let limit = limit.min(self.cap.saturating_duration_since(Instant::now()));
        self.run_uncapped(args, limit)
    }

    fn exec(&self, service: &str, argv: &[&str]) -> Output {
        self.run(&exec_args(service, argv), EXEC_LIMIT)
    }

    fn down(&self) {
        let out = self.run_uncapped(
            &["down", "-v", "--remove-orphans", "--timeout", "5"],
            DOWN_LIMIT,
        );
        if !out.ok {
            eprintln!(
                "teardown of {} failed; remove it by hand:\n{}",
                self.project, out.stderr
            );
        }
    }

    /// Seeder log, the seeder's current answer, the client's log and address book.
    fn dump(&self, gate: &Gate) {
        let seeder = self.run_uncapped(&["logs", "--no-color", gate.seeder_service], DUMP_LIMIT);
        println!(
            "===== {} log =====\n{}{}",
            gate.seeder_service, seeder.stdout, seeder.stderr
        );
        let dig = self.run_uncapped(
            &exec_args(
                gate.seeder_service,
                &[
                    "dig",
                    "+time=1",
                    "+tries=1",
                    "@127.0.0.1",
                    gate.seed_query,
                    "A",
                ],
            ),
            DUMP_LIMIT,
        );
        println!(
            "===== {} answer for {} =====\n{}{}",
            gate.seeder_service, gate.seed_query, dig.stdout, dig.stderr
        );
        let log = self.run_uncapped(
            &exec_args(gate.client_service, &["cat", gate.client_log]),
            DUMP_LIMIT,
        );
        println!(
            "===== {} {} =====\n{}{}",
            gate.client_service, gate.client_log, log.stdout, log.stderr
        );
        let addrs = self.run_uncapped(
            &exec_args(gate.client_service, &cli(gate, gate.probe)),
            DUMP_LIMIT,
        );
        println!(
            "===== {} {} =====\n{}{}",
            gate.client_service,
            gate.probe.join(" "),
            addrs.stdout,
            addrs.stderr
        );
    }
}

/// Tears the project down on every path out of the test, panics included.
struct Teardown(Compose);

impl Drop for Teardown {
    fn drop(&mut self) {
        self.0.down();
    }
}

fn cli<'a>(gate: &Gate, call: &[&'a str]) -> Vec<&'a str> {
    let mut argv: Vec<&str> = gate.client_cli.to_vec();
    argv.extend_from_slice(call);
    argv
}

/// The counts on the client's `N addresses found from DNS seeds` lines.
fn dns_seed_counts(log: &str) -> Vec<u64> {
    log.lines()
        .filter_map(|line| {
            let at = line.find(" addresses found from DNS seeds")?;
            line[..at].split_whitespace().last()?.parse().ok()
        })
        .collect()
}

enum Verdict {
    Pass {
        found: u64,
        learned: Vec<&'static str>,
    },
    Fail(String),
}

/// Checks the client once. `None` means not decided yet.
fn check(compose: &Compose, gate: &Gate) -> Option<Verdict> {
    let log = compose.exec(gate.client_service, &["cat", gate.client_log]);
    if !log.ok {
        return None;
    }
    let found = dns_seed_counts(&log.stdout).into_iter().max()?;
    if found == 0 {
        // The DNS seed thread runs once per start; a zero count is final.
        return Some(Verdict::Fail(
            "the client's DNS seeding found 0 addresses".into(),
        ));
    }
    let addrs = compose.exec(gate.client_service, &cli(gate, gate.probe));
    if !addrs.ok {
        return None;
    }
    let learned: Vec<&'static str> = gate
        .nodes
        .iter()
        .copied()
        .filter(|ip| {
            addrs
                .stdout
                .contains(&format!("\"{ip}{}", gate.probe_after_ip))
        })
        .collect();
    if learned.is_empty() {
        return None;
    }
    Some(Verdict::Pass { found, learned })
}

fn run(gate: &Gate) {
    let started = Instant::now();
    let cap = started + HARD_CAP - TEARDOWN_MARGIN;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let compose = Compose {
        project: format!(
            "seedcrystal-e2e-{}-{}-{nanos}",
            gate.coin,
            std::process::id()
        ),
        file: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(gate.compose_file),
        cap,
    };
    let version = bounded(
        {
            let mut c = Command::new("docker");
            c.args(["compose", "version"]);
            c
        },
        Instant::now() + EXEC_LIMIT,
    );
    assert!(
        version.ok,
        "docker compose is not available: {}",
        version.stderr
    );

    let _teardown = Teardown(compose.clone());
    // Dropping the sender (at the end of the test, or while unwinding) stops the watchdog.
    let (_done, done_rx) = mpsc::channel::<()>();
    let watched = compose.clone();
    thread::spawn(move || {
        if let Err(RecvTimeoutError::Timeout) = done_rx.recv_timeout(HARD_CAP) {
            eprintln!(
                "e2e gate hit its {}-minute hard cap; tearing down {}",
                HARD_CAP.as_secs() / 60,
                watched.project
            );
            watched.down();
            std::process::exit(1);
        }
    });

    println!("compose project {}", compose.project);
    let up = compose.run(&["up", "-d", "--build"], HARD_CAP);
    if !up.ok {
        compose.dump(gate);
        panic!("docker compose up failed:\n{}", up.stderr);
    }
    let up_at = started.elapsed();
    println!("stack up after {:.0?}", up_at);

    let deadline = (Instant::now() + POLL_LIMIT).min(cap);
    let verdict = loop {
        if let Some(v) = check(&compose, gate) {
            break v;
        }
        if Instant::now() + POLL_EVERY >= deadline {
            break Verdict::Fail(format!(
                "no DNS-seeded node address on the client within {:.0?} of the stack starting",
                POLL_LIMIT
            ));
        }
        thread::sleep(POLL_EVERY);
    };
    compose.dump(gate);
    match verdict {
        Verdict::Pass { found, learned } => println!(
            "PASS after {:.0?} (stack up at {:.0?}): {found} addresses found from DNS seeds; \
             {} shows {learned:?}",
            started.elapsed(),
            up_at,
            gate.probe[0]
        ),
        Verdict::Fail(why) => panic!("FAIL after {:.0?}: {why}", started.elapsed()),
    }
}

#[test]
fn dns_seed_counts_reads_the_log_line() {
    let log = "2026-10-25T11:29:05Z Loading addresses from DNS seed testnet-seed.osmium.space\n\
               2026-10-25T11:29:05Z 3 addresses found from DNS seeds\n\
               2026-10-25T11:29:05Z dnsseed thread exit\n";
    assert_eq!(dns_seed_counts(log), vec![3]);
    assert_eq!(
        dns_seed_counts("2026-10-25T11:29:05Z 0 addresses found from DNS seeds\n"),
        vec![0]
    );
    assert!(dns_seed_counts("2026-10-25T11:29:05Z dnsseed thread start\n").is_empty());
}
