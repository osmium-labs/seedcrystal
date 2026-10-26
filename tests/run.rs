mod support;

use std::io::Read;
use std::net::{SocketAddr, UdpSocket};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use hickory_proto::op::{Message, MessageType, OpCode, Query};
use hickory_proto::rr::{Name, RData, RecordType};
use seedcrystal::coin::builtin;
use support::fake_cloudflare::FakeCloudflare;
use support::{FakePeer, FakeScript};

/// Kills the child on every path, including panics.
struct Guard(Child);

impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    loop {
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = tcp.local_addr().unwrap().port();
        if UdpSocket::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

fn query_a(server: SocketAddr, name: &str) -> Option<Vec<std::net::Ipv4Addr>> {
    let mut msg = Message::new(7, MessageType::Query, OpCode::Query);
    msg.add_query(Query::query(Name::from_ascii(name).unwrap(), RecordType::A));
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    sock.send_to(&msg.to_vec().unwrap(), server).ok()?;
    let mut buf = [0u8; 1500];
    let n = sock.recv(&mut buf).ok()?;
    let reply = Message::from_vec(&buf[..n]).ok()?;
    Some(
        reply
            .answers
            .iter()
            .filter_map(|r| match &r.data {
                RData::A(a) => Some(a.0),
                _ => None,
            })
            .collect(),
    )
}

/// The built-in preset's bootstrap points at real nodes; a test must never dial them.
fn write_fake_preset(dir: &std::path::Path) {
    let mut text = include_str!("../presets/osmium-mainnet.toml").to_string();
    for key in ["bootstrap_dns", "bootstrap_fixed"] {
        let start = text.find(&format!("{key} =")).unwrap();
        let end = start + text[start..].find('\n').unwrap();
        text.replace_range(start..end, &format!("{key} = []"));
    }
    std::fs::write(dir.join("fake.toml"), text).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn run_serves_a_fake_network() {
    let preset = builtin("osmium-mainnet").unwrap();
    let mut script = FakeScript::new(preset.magic, preset.protocol_version);
    // Closing after getaddr ends the probe at once; otherwise it waits out the full addr_wait.
    script.close_after_getaddr = true;
    let (peer, _handle) = FakePeer::spawn(script);

    let dir = tempfile::tempdir().unwrap();
    write_fake_preset(dir.path());
    let port = free_port();
    let cfg = dir.path().join("seedcrystal.toml");
    std::fs::write(
        &cfg,
        format!(
            r#"
[dns]
listen = ["127.0.0.1:{port}"]

[[network]]
preset_file = "fake.toml"
seed = "dnsseed.osmium.space"
nameserver = "seed-ns.osmium.space"
mailbox = "hostmaster.osmium.space"
data_dir = "{}"
bootstrap = ["{peer}"]
allow_private = true
"#,
            dir.path().join("data").display()
        ),
    )
    .unwrap();

    let mut child = Guard(
        Command::new(env!("CARGO_BIN_EXE_seedcrystal"))
            .args(["run", "--config"])
            .arg(&cfg)
            .env_remove("SEEDCRYSTAL_DNS_LISTEN")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );

    let server: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let started = Instant::now();
    let deadline = started + Duration::from_secs(15);
    let found = tokio::task::spawn_blocking(move || {
        while Instant::now() < deadline {
            if let Some(ips) = query_a(server, "x1.dnsseed.osmium.space.")
                && ips.contains(&"127.0.0.1".parse().unwrap())
            {
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        false
    })
    .await
    .unwrap();
    if !found {
        let _ = child.0.kill();
        let mut log = String::new();
        let _ = child.0.stderr.take().unwrap().read_to_string(&mut log);
        panic!("x1.dnsseed.osmium.space never answered 127.0.0.1; seedcrystal stderr:\n{log}");
    }
    let took = started.elapsed();
    eprintln!("answered after {took:?}");
    assert!(
        took < Duration::from_secs(8),
        "answer took {took:?}; the fake closes after getaddr, so this should be a few seconds"
    );

    // SIGTERM stops it cleanly with exit code 0.
    let pid = child.0.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let end = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(s) = child.0.try_wait().unwrap() {
            break s;
        }
        assert!(Instant::now() < end, "did not exit within 10 s of SIGTERM");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(status.success(), "exit status {status}");

    // The shutdown writes a final query log.
    let log = seedcrystal::dns::querylog::read(
        &dir.path().join("data").join("queries-osmium-mainnet.json"),
    )
    .unwrap();
    let stats = &log.names["x1.dnsseed.osmium.space"];
    assert!(stats.answered >= 1, "{log:?}");
}

fn run_to_exit(cfg: &std::path::Path, listen: &str) -> (std::process::ExitStatus, String) {
    let child = Command::new(env!("CARGO_BIN_EXE_seedcrystal"))
        .args(["run", "--config"])
        .arg(cfg)
        .env("SEEDCRYSTAL_DNS_LISTEN", listen)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut guard = Guard(child);
    let mut err = guard.0.stderr.take().unwrap();
    let reader = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = err.read_to_string(&mut s);
        s
    });
    let end = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(s) = guard.0.try_wait().unwrap() {
            break s;
        }
        assert!(Instant::now() < end, "seedcrystal did not exit by itself");
        std::thread::sleep(Duration::from_millis(50));
    };
    (status, reader.join().unwrap())
}

fn two_networks(dir: &std::path::Path, second_preset: &str) -> std::path::PathBuf {
    let cfg = dir.join("two.toml");
    write_fake_preset(dir);
    std::fs::write(
        &cfg,
        format!(
            r#"
[[network]]
preset_file = "fake.toml"
seed = "a.osmium.space"
nameserver = "ns.osmium.space"
mailbox = "hostmaster.osmium.space"
data_dir = "{d}/a"
bootstrap = ["127.0.0.1:1"]
allow_private = true

[[network]]
{second_preset}
seed = "b.osmium.space"
nameserver = "ns.osmium.space"
mailbox = "hostmaster.osmium.space"
data_dir = "{d}/b"
"#,
            d = dir.display()
        ),
    )
    .unwrap();
    cfg
}

#[test]
fn bad_second_network_exits_non_zero_with_its_name() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = two_networks(dir.path(), "preset_file = \"nope.toml\"");
    let (status, log) = run_to_exit(&cfg, "127.0.0.1:0");
    assert!(!status.success());
    assert!(log.contains("nope.toml"), "{log}");
}

#[test]
fn port_in_use_exits_non_zero_and_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = held.local_addr().unwrap();
    let cfg = dir.path().join("one.toml");
    write_fake_preset(dir.path());
    std::fs::write(
        &cfg,
        format!(
            "[[network]]\npreset_file = \"fake.toml\"\nseed = \"a.osmium.space\"\n\
             nameserver = \"ns.osmium.space\"\nmailbox = \"hostmaster.osmium.space\"\n\
             data_dir = \"{}/a\"\n",
            dir.path().display()
        ),
    )
    .unwrap();
    let (status, log) = run_to_exit(&cfg, &addr.to_string());
    assert!(!status.success());
    assert!(log.contains("already in use"), "{log}");
}

const CF_TOKEN: &str = "s3cret-token-value";

fn cloudflare_config(dir: &std::path::Path, port: u16, api_base: &str, peer: SocketAddr) -> String {
    write_fake_preset(dir);
    let cfg = dir.join("cf.toml");
    std::fs::write(
        &cfg,
        format!(
            r#"
[dns]
listen = ["127.0.0.1:{port}"]

[[network]]
preset_file = "fake.toml"
seed = "dnsseed.osmium.space"
data_dir = "{}"
bootstrap = ["{peer}"]
allow_private = true
publish = "cloudflare"

[network.cloudflare]
zone_id = "zone1"
api_base = "{api_base}"
"#,
            dir.join("data").display()
        ),
    )
    .unwrap();
    cfg.display().to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn run_publishes_to_fake_cloudflare() {
    let fake = FakeCloudflare::start("zone1", CF_TOKEN).await;
    let preset = builtin("osmium-mainnet").unwrap();
    let mut script = FakeScript::new(preset.magic, preset.protocol_version);
    script.close_after_getaddr = true;
    let (peer, _handle) = FakePeer::spawn(script);

    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let cfg = cloudflare_config(dir.path(), port, &fake.base_url(), peer);
    let mut child = Guard(
        Command::new(env!("CARGO_BIN_EXE_seedcrystal"))
            .args(["run", "--config", &cfg])
            .env_remove("SEEDCRYSTAL_DNS_LISTEN")
            .env("SEEDCRYSTAL_CLOUDFLARE_TOKEN", CF_TOKEN)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );

    let deadline = Instant::now() + Duration::from_secs(15);
    let published = loop {
        let found = fake.records().into_iter().find(|r| {
            r.name == "x1.dnsseed.osmium.space" && r.kind == "A" && r.content == "127.0.0.1"
        });
        if found.is_some() || Instant::now() > deadline {
            break found;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    if published.is_none() {
        let _ = child.0.kill();
        let mut log = String::new();
        let _ = child.0.stderr.take().unwrap().read_to_string(&mut log);
        panic!("no A record for x1.dnsseed.osmium.space; seedcrystal stderr:\n{log}");
    }
    assert_eq!(
        published.unwrap().comment.as_deref(),
        Some("managed by seedcrystal")
    );

    // No DNS socket is bound in Cloudflare mode.
    UdpSocket::bind(("127.0.0.1", port)).expect("udp port must be free");
    std::net::TcpListener::bind(("127.0.0.1", port)).expect("tcp port must be free");

    let pid = child.0.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let end = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(s) = child.0.try_wait().unwrap() {
            break s;
        }
        assert!(Instant::now() < end, "did not exit within 10 s of SIGTERM");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(status.success(), "exit status {status}");
}

#[tokio::test(flavor = "multi_thread")]
async fn rejected_token_stops_run_with_a_message_and_never_echoes_it() {
    let fake = FakeCloudflare::start("zone1", CF_TOKEN).await;
    let preset = builtin("osmium-mainnet").unwrap();
    let mut script = FakeScript::new(preset.magic, preset.protocol_version);
    script.close_after_getaddr = true;
    let (peer, _handle) = FakePeer::spawn(script);
    let dir = tempfile::tempdir().unwrap();
    let cfg = cloudflare_config(dir.path(), free_port(), &fake.base_url(), peer);
    let child = Command::new(env!("CARGO_BIN_EXE_seedcrystal"))
        .args(["run", "--config", &cfg])
        .env("SEEDCRYSTAL_CLOUDFLARE_TOKEN", "wrong-token-value")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let out = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    let log = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{log}");
    assert!(log.contains("Zone"), "{log}");
    assert!(!log.contains("wrong-token-value"), "{log}");
}

#[test]
fn missing_token_exits_non_zero_naming_both_ways_to_give_it() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = cloudflare_config(
        dir.path(),
        free_port(),
        "http://127.0.0.1:1",
        "127.0.0.1:1".parse().unwrap(),
    );
    let child = Command::new(env!("CARGO_BIN_EXE_seedcrystal"))
        .args(["run", "--config", &cfg])
        .env_remove("SEEDCRYSTAL_CLOUDFLARE_TOKEN")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let out = child.wait_with_output().unwrap();
    let log = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(
        log.contains("token_file") && log.contains("SEEDCRYSTAL_CLOUDFLARE_TOKEN"),
        "{log}"
    );
}

#[test]
fn presets_lists_every_builtin_with_port_seed_name_and_protocol() {
    let out = Command::new(env!("CARGO_BIN_EXE_seedcrystal"))
        .arg("presets")
        .output()
        .unwrap();
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).unwrap();
    let on_disk = std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/presets"))
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "toml")
        })
        .count();
    assert_eq!(text.lines().count(), on_disk, "{text}");
    let osmium: Vec<&str> = text
        .lines()
        .find(|l| l.starts_with("osmium-mainnet "))
        .unwrap_or_else(|| panic!("no osmium-mainnet line in {text}"))
        .split_whitespace()
        .collect();
    assert_eq!(
        osmium,
        ["osmium-mainnet", "port", "9969", "x1", "protocol", "70231"]
    );
}
