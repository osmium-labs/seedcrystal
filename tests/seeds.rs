use std::net::SocketAddr;
use std::process::Command;

use seedcrystal::p2p::ChainCheck;
use seedcrystal::store::{NodeStore, Outcome};

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Three successes 10 minutes apart, ending now: meets the full bar.
fn seed_good(store: &NodeStore, addr: &str) {
    let addr: SocketAddr = addr.parse().unwrap();
    let now = unix_now();
    for i in 0..3 {
        let outcome = Outcome::Good {
            version: 70231,
            services: 1,
            height: 1000,
            harvested: false,
            chain: ChainCheck::NotChecked,
        };
        store
            .record(addr, outcome, now - (2 - i) * 600, None)
            .unwrap();
    }
}

fn write_config(dir: &std::path::Path, extra_network: bool) -> std::path::PathBuf {
    let mut text = format!(
        r#"
[dns]
listen = ["127.0.0.1:5353"]

[[network]]
preset = "osmium-mainnet"
seed = "dnsseed.example.org"
nameserver = "ns.example.org"
mailbox = "hostmaster.example.org"
data_dir = "{}"
allow_private = true
"#,
        dir.join("data").display()
    );
    if extra_network {
        text.push_str(&format!(
            r#"
[[network]]
preset = "osmium-mainnet"
seed = "other.example.org"
nameserver = "ns.example.org"
mailbox = "hostmaster.example.org"
data_dir = "{}"
allow_private = true
"#,
            dir.join("data2").display()
        ));
    }
    let cfg = dir.join("seedcrystal.toml");
    std::fs::write(&cfg, text).unwrap();
    cfg
}

fn run(cfg: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_seedcrystal"))
        .args(["seeds", "--config"])
        .arg(cfg)
        .args(args)
        .env_remove("SEEDCRYSTAL_DNS_LISTEN")
        .output()
        .unwrap()
}

#[test]
fn seeds_command_reads_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), false);
    std::fs::create_dir_all(dir.path().join("data")).unwrap();
    let store = NodeStore::open(&dir.path().join("data/osmium-mainnet.sqlite")).unwrap();
    for a in [
        "203.0.113.1:9969",
        "203.0.113.2:9969",
        "203.0.113.3:9969",
        "198.51.100.7:9969",
        "[2001:db8::1]:9969",
    ] {
        seed_good(&store, a);
    }
    drop(store);
    // Servable order is by the address text: "198..." < "2001:..." < "203...".

    let out = run(&cfg, &[]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        String::from_utf8(out.stdout).unwrap(),
        "198.51.100.7:9969\n[2001:db8::1]:9969\n203.0.113.1:9969\n203.0.113.2:9969\n"
    );

    let out = run(&cfg, &["--max", "1"]);
    assert_eq!(
        String::from_utf8(out.stdout).unwrap(),
        "198.51.100.7:9969\n"
    );
}

#[test]
fn seeds_needs_a_network_when_there_are_several() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), true);
    let out = run(&cfg, &[]);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("--network"), "{err}");
    assert!(
        err.contains("dnsseed.example.org") && err.contains("other.example.org"),
        "{err}"
    );

    let out = run(&cfg, &["--network", "nope.example.org"]);
    assert!(!out.status.success());
}

#[test]
fn seeds_without_a_crawl_database_fails_and_creates_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), false);
    let out = run(&cfg, &[]);
    assert!(!out.status.success());
    assert!(!dir.path().join("data").exists());
}

fn dir_listing(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn seeds_refuses_another_schema_and_leaves_the_store_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), false);
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let path = data.join("osmium-mainnet.sqlite");
    let c = rusqlite::Connection::open(&path).unwrap();
    c.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
        .unwrap();
    // `run` would adopt this file and stamp it with the current version; `seeds` must not.
    c.execute_batch("CREATE TABLE nodes (ip TEXT NOT NULL)")
        .unwrap();
    c.pragma_update(None, "user_version", 1).unwrap();
    drop(c);
    let before = std::fs::read(&path).unwrap();
    let files = dir_listing(&data);

    let out = run(&cfg, &[]);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("is schema 1"), "{err}");
    assert!(err.contains(&path.display().to_string()), "{err}");
    assert!(out.stdout.is_empty());

    assert_eq!(std::fs::read(&path).unwrap(), before, "store file changed");
    assert_eq!(dir_listing(&data), files, "files appeared or vanished");
}
