mod support;

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use seedcrystal::coin::{Checksum, CoinPreset, builtin, from_toml};
use seedcrystal::p2p::{
    AddrEntry, ChainCheck, NetAddr, ProbeError, ProbeOptions, ProbeResult, decode_ping,
    decode_version, encode_ping, probe,
};
use support::{FakePeer, FakeScript};

fn osmium() -> CoinPreset {
    builtin("osmium-mainnet").unwrap()
}

fn bitcoin() -> CoinPreset {
    builtin("bitcoin-mainnet").unwrap()
}

fn opts() -> ProbeOptions {
    ProbeOptions {
        handshake_timeout: Duration::from_secs(5),
        addr_wait: Duration::from_millis(500),
        ..ProbeOptions::default()
    }
}

fn inline(extra: &str) -> CoinPreset {
    from_toml(&format!(
        r#"
id = "examplecoin-mainnet"
magic = "a1b2c3d4"
default_port = 12345
protocol_version = 70016
min_peer_version = 70001
addrv2_min_version = 70016
block_interval_secs = 600
seed_flags = 0x1
filters = [0x1]
bootstrap_dns = ["seed.example.org"]
bootstrap_fixed = []
{extra}
"#
    ))
    .unwrap()
}

fn entry(last: u8) -> AddrEntry {
    AddrEntry {
        addr: NetAddr::V4(Ipv4Addr::new(203, 0, 113, last)),
        port: 9000,
        services: 1,
        time: 1_700_000_000,
    }
}

async fn bounded<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(15), f)
        .await
        .expect("test exceeded 15 s")
}

async fn run(
    script: FakeScript,
    preset: &CoinPreset,
    o: &ProbeOptions,
) -> (Result<ProbeResult, ProbeError>, Vec<String>) {
    let (addr, fake) = FakePeer::spawn(script);
    let result = probe(addr, preset, o).await;
    (result, fake.await.unwrap())
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn happy_path_returns_peer_version_and_addrs() {
    bounded(async {
        let p = osmium();
        let mut s = FakeScript::new(p.magic, 70231);
        s.addr_reply = vec![entry(1), entry(2)];
        let o = ProbeOptions {
            want_addrs: true,
            ..opts()
        };
        let (res, seen) = run(s, &p, &o).await;
        let res = res.unwrap();
        assert_eq!(res.peer.version, 70231);
        assert_eq!(res.addrs.len(), 2);
        assert_eq!(res.addrs, vec![entry(1), entry(2)]);
        // sendaddrv2 is negotiated at 70231, so it sits between version and verack.
        let seen: Vec<_> = seen.into_iter().filter(|c| c != "sendaddrv2").collect();
        assert!(
            seen.starts_with(&strs(&["version", "verack", "getaddr"])),
            "{seen:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn addrv2_replies_are_collected() {
    bounded(async {
        let p = osmium();
        let mut s = FakeScript::new(p.magic, 70231);
        s.addr_reply = vec![entry(3)];
        s.use_addrv2 = true;
        let o = ProbeOptions {
            want_addrs: true,
            ..opts()
        };
        let (res, _) = run(s, &p, &o).await;
        assert_eq!(res.unwrap().addrs, vec![entry(3)]);
    })
    .await;
}

#[tokio::test]
async fn dash_extras_and_unknown_messages_are_tolerated() {
    bounded(async {
        let p = osmium();
        let mut s = FakeScript::new(p.magic, 70231);
        s.trailing = vec![0xab; 33];
        s.after_verack = ["sendheaders2", "qwatch", "senddsq", "mnauth"]
            .iter()
            .map(|c| (c.to_string(), vec![1, 2, 3]))
            .collect();
        let (res, _) = run(s, &p, &opts()).await;
        assert!(res.is_ok(), "{res:?}");
    })
    .await;
}

#[tokio::test]
async fn version_below_floor_is_rejected() {
    bounded(async {
        let p = osmium();
        let (res, _) = run(FakeScript::new(p.magic, 70214), &p, &opts()).await;
        assert!(
            matches!(res, Err(ProbeError::VersionTooLow(70214))),
            "{res:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn silent_peer_times_out() {
    bounded(async {
        let p = osmium();
        let mut s = FakeScript::new(p.magic, 70231);
        s.silent = true;
        let o = ProbeOptions {
            handshake_timeout: Duration::from_millis(300),
            ..opts()
        };
        let start = Instant::now();
        let (res, _) = run(s, &p, &o).await;
        assert!(matches!(res, Err(ProbeError::Timeout)), "{res:?}");
        assert!(start.elapsed() < Duration::from_secs(1));
    })
    .await;
}

#[tokio::test]
async fn drip_feeding_peer_times_out() {
    bounded(async {
        let p = osmium();
        let mut s = FakeScript::new(p.magic, 70231);
        s.drip = true;
        let o = ProbeOptions {
            handshake_timeout: Duration::from_millis(600),
            ..opts()
        };
        let start = Instant::now();
        let (res, _) = run(s, &p, &o).await;
        assert!(matches!(res, Err(ProbeError::Timeout)), "{res:?}");
        assert!(start.elapsed() < Duration::from_millis(600 + 500));
    })
    .await;
}

#[tokio::test]
async fn sendaddrv2_only_when_negotiated() {
    bounded(async {
        let cases = [
            (osmium(), 70231, true),
            (osmium(), 70222, false),
            (bitcoin(), 70016, true),
        ];
        for (p, theirs, expect) in cases {
            let (res, seen) = run(FakeScript::new(p.magic, theirs), &p, &opts()).await;
            res.unwrap();
            let pos = seen.iter().position(|c| c == "sendaddrv2");
            assert_eq!(pos.is_some(), expect, "{} vs {theirs}: {seen:?}", p.id);
            if let Some(pos) = pos {
                let verack = seen.iter().position(|c| c == "verack").unwrap();
                assert!(pos < verack, "{seen:?}");
            }
        }
    })
    .await;
}

#[tokio::test]
async fn no_getaddr_unless_asked() {
    bounded(async {
        let p = osmium();
        let mut s = FakeScript::new(p.magic, 70231);
        s.addr_reply = vec![entry(1)];
        let (res, seen) = run(s, &p, &opts()).await;
        assert!(res.unwrap().addrs.is_empty());
        assert!(!seen.iter().any(|c| c == "getaddr"), "{seen:?}");
    })
    .await;
}

#[tokio::test]
async fn answers_ping_while_waiting_for_addrs() {
    bounded(async {
        let p = osmium();
        let mut s = FakeScript::new(p.magic, 70231);
        s.after_verack = vec![("ping".into(), encode_ping(0xfeed))];
        s.addr_reply = vec![entry(1)];
        let o = ProbeOptions {
            want_addrs: true,
            ..opts()
        };
        let (addr, fake) = FakePeer::spawn_frames(s);
        let res = probe(addr, &p, &o).await.unwrap();
        assert_eq!(res.addrs.len(), 1);
        let seen = fake.await.unwrap();
        let pong = seen.iter().find(|(c, _)| c == "pong").expect("no pong");
        assert_eq!(decode_ping(&pong.1).unwrap(), 0xfeed);
    })
    .await;
}

#[tokio::test]
async fn connection_refused_is_a_connect_error() {
    bounded(async {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = l.local_addr().unwrap();
        drop(l);
        let res = probe(addr, &osmium(), &opts()).await;
        assert!(matches!(res, Err(ProbeError::Connect(_))), "{res:?}");
    })
    .await;
}

#[tokio::test]
async fn ping_flood_without_reading_cannot_stall_collection() {
    bounded(async {
        let p = osmium();
        let mut s = FakeScript::new(p.magic, 70231);
        s.flood_pings = true;
        let o = ProbeOptions {
            want_addrs: true,
            addr_wait: Duration::from_millis(500),
            ..opts()
        };
        let start = Instant::now();
        let (res, _) = run(s, &p, &o).await;
        assert!(res.unwrap().addrs.is_empty());
        assert!(start.elapsed() < Duration::from_millis(500 + 1000));
    })
    .await;
}

#[tokio::test]
async fn our_version_message_is_conservative() {
    bounded(async {
        for p in [osmium(), bitcoin()] {
            let (addr, fake) = FakePeer::spawn_frames(FakeScript::new(p.magic, 70231));
            probe(addr, &p, &opts()).await.unwrap();
            let seen = fake.await.unwrap();
            let (_, payload) = seen.iter().find(|(c, _)| c == "version").unwrap();
            let v = decode_version(payload).unwrap();
            assert_eq!(v.version, p.protocol_version);
            assert_eq!(v.services, 0);
            assert!(!v.relay);
            assert!(
                v.user_agent.starts_with("/seedcrystal:"),
                "{}",
                v.user_agent
            );
            assert_eq!(v.receiver, addr);
            if p.version_trailer == seedcrystal::coin::VersionTrailer::Dash {
                // 32-byte challenge, then the masternode flag, which must stay false.
                assert_eq!(*payload.last().unwrap(), 0x00);
            }
        }
    })
    .await;
}

#[tokio::test]
async fn oversize_post_handshake_frame_is_skipped_not_fatal() {
    bounded(async {
        let p = osmium();
        let mut s = FakeScript::new(p.magic, 70231);
        s.after_verack = vec![("inv".into(), vec![7u8; 2 << 20])];
        s.addr_reply = vec![entry(1), entry(2)];
        let o = ProbeOptions {
            want_addrs: true,
            ..opts()
        };
        let (addr, _fake) = FakePeer::spawn_frames(s);
        let res = probe(addr, &p, &o).await.unwrap();
        assert_eq!(res.addrs.len(), 2);
    })
    .await;
}

#[tokio::test]
async fn default_addr_wait_is_30_seconds() {
    assert_eq!(ProbeOptions::default().addr_wait, Duration::from_secs(30));
}

#[tokio::test]
async fn after_verack_placement_satisfies_a_strict_peer() {
    bounded(async {
        let p = inline("version_trailer = \"none\"\naddrv2_placement = \"after_verack\"");
        let mut s = FakeScript::new(p.magic, 70016);
        s.strict_pre_verack = true;
        let (res, seen) = run(s, &p, &opts()).await;
        assert!(res.is_ok(), "{res:?}");
        assert_eq!(seen, strs(&["version", "verack", "sendaddrv2"]));
    })
    .await;
}

#[tokio::test]
async fn before_verack_placement_trips_a_strict_peer() {
    bounded(async {
        let p = inline("version_trailer = \"none\"");
        let mut s = FakeScript::new(p.magic, 70016);
        s.strict_pre_verack = true;
        let (res, seen) = run(s, &p, &opts()).await;
        assert!(matches!(res, Err(ProbeError::Closed)), "{res:?}");
        assert_eq!(seen, strs(&["version", "sendaddrv2"]));
    })
    .await;
}

#[tokio::test]
async fn handshake_answers_ping_and_ignores_unknown_messages_before_verack() {
    bounded(async {
        let p = bitcoin();
        let mut s = FakeScript::new(p.magic, 70016);
        let nonce = 0x1122_3344_5566_7788u64;
        s.between_version_and_verack = vec![
            ("sendaddrv2".to_string(), Vec::new()),
            ("wtxidrelay".to_string(), Vec::new()),
            ("ping".to_string(), nonce.to_le_bytes().to_vec()),
        ];
        let (addr, fake) = FakePeer::spawn_frames(s);
        let res = probe(addr, &p, &opts()).await;
        let received = fake.await.unwrap();
        assert!(res.is_ok(), "{res:?}");
        let pongs: Vec<&Vec<u8>> = received
            .iter()
            .filter(|(c, _)| c == "pong")
            .map(|(_, payload)| payload)
            .collect();
        assert_eq!(pongs, vec![&nonce.to_le_bytes().to_vec()]);
    })
    .await;
}

#[tokio::test]
async fn none_trailer_passes_a_challenge_checking_peer() {
    bounded(async {
        let none = inline("version_trailer = \"none\"");
        let mut s = FakeScript::new(none.magic, 70016);
        s.reject_nonzero_challenge = true;
        let (res, _) = run(s.clone(), &none, &opts()).await;
        assert!(res.is_ok(), "{res:?}");

        let dash = inline("version_trailer = \"dash\"");
        let (res, _) = run(s, &dash, &opts()).await;
        assert!(matches!(res, Err(ProbeError::Closed)), "{res:?}");
    })
    .await;
}

#[tokio::test]
async fn delayed_reply_is_collected_within_addr_wait() {
    bounded(async {
        let p = inline("version_trailer = \"none\"");
        let mut s = FakeScript::new(p.magic, 70016);
        s.addr_delay = Duration::from_secs(2);
        s.addr_reply = vec![entry(1), entry(2)];
        let o = ProbeOptions {
            want_addrs: true,
            addr_wait: Duration::from_secs(5),
            ..opts()
        };
        let start = Instant::now();
        let (res, _) = run(s, &p, &o).await;
        assert_eq!(res.unwrap().addrs, vec![entry(1), entry(2)]);
        // 2 s delay + 1 s grace after the multi-entry message (~3 s), not the full 5 s wait;
        // the bound leaves room for a busy machine while still failing a full wait.
        assert!(
            start.elapsed() < Duration::from_millis(4900),
            "{:?}",
            start.elapsed()
        );
    })
    .await;
}

#[tokio::test]
async fn single_entry_addr_does_not_end_collection() {
    bounded(async {
        let p = inline("version_trailer = \"none\"");
        let mut s = FakeScript::new(p.magic, 70016);
        s.relay_flush = vec![entry(9)];
        s.addr_delay = Duration::from_secs(2);
        s.addr_reply = vec![entry(1), entry(2), entry(3)];
        let o = ProbeOptions {
            want_addrs: true,
            addr_wait: Duration::from_secs(5),
            ..opts()
        };
        let (res, _) = run(s, &p, &o).await;
        let addrs = res.unwrap().addrs;
        assert_eq!(addrs.len(), 4, "{addrs:?}");
        assert_eq!(addrs[0], entry(9));
    })
    .await;
}

#[tokio::test]
async fn groestl_checksum_peer_handshakes() {
    bounded(async {
        let groestl = inline("version_trailer = \"none\"\nchecksum = \"groestl512d\"");
        let mut s = FakeScript::new(groestl.magic, 70016);
        s.checksum = Checksum::Groestl512d;
        let (res, _) = run(s.clone(), &groestl, &opts()).await;
        assert!(res.is_ok(), "{res:?}");

        let sha = inline("version_trailer = \"none\"");
        let (res, _) = run(s, &sha, &opts()).await;
        assert!(matches!(res, Err(ProbeError::Wire(_))), "{res:?}");
    })
    .await;
}

/// Explorer order, as a preset writes it.
const CHECKPOINT_HEX: &str = "00000000000000000001a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f7";
const CHECKPOINT_HEIGHT: i32 = 50;

fn checkpointed() -> CoinPreset {
    inline(&format!(
        "version_trailer = \"none\"\ncheckpoint = {{ height = {CHECKPOINT_HEIGHT}, hash = \"{CHECKPOINT_HEX}\" }}"
    ))
}

/// The checkpoint hash in wire order, derived here independently of the preset parser.
fn wire_hash() -> [u8; 32] {
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&CHECKPOINT_HEX[i * 2..i * 2 + 2], 16).unwrap();
    }
    out.reverse();
    out
}

/// A `headers` payload: `count`, then 81-byte headers (80 + empty tx count) whose first one
/// builds on `prev`, padded with zeros to at least `min_len` bytes.
fn headers(count: u16, prev: [u8; 32], min_len: usize) -> Vec<u8> {
    let mut out = if count < 0xfd {
        vec![count as u8]
    } else {
        let mut v = vec![0xfd];
        v.extend(count.to_le_bytes());
        v
    };
    if count > 0 {
        out.extend(4i32.to_le_bytes());
        out.extend(prev);
        out.extend([0u8; 44]); // merkle root, time, bits, nonce
        out.push(0);
        out.resize(out.len() + 81 * (count as usize - 1), 0);
    }
    if out.len() < min_len {
        out.resize(min_len, 0);
    }
    out
}

fn checking() -> ProbeOptions {
    ProbeOptions {
        check_chain: true,
        ..opts()
    }
}

#[tokio::test]
async fn checkpoint_hash_is_reversed_on_the_wire() {
    bounded(async {
        let p = checkpointed();
        let mut s = FakeScript::new(p.magic, 70016);
        s.headers_reply = Some(headers(1, wire_hash(), 0));
        s.addr_reply = vec![entry(1), entry(2)];
        let o = ProbeOptions {
            want_addrs: true,
            ..checking()
        };
        let (addr, fake) = FakePeer::spawn_frames(s);
        let res = probe(addr, &p, &o).await.unwrap();
        assert_eq!(res.chain, ChainCheck::Verified);
        assert_eq!(res.addrs, vec![entry(1), entry(2)]);
        let seen = fake.await.unwrap();
        let pos = |cmd: &str| seen.iter().position(|(c, _)| c == cmd);
        let gh = pos("getheaders").expect("no getheaders");
        assert!(
            gh < pos("getaddr").expect("no getaddr"),
            "getheaders goes first"
        );
        let payload = &seen[gh].1;
        assert_eq!(&payload[..4], &p.protocol_version.to_le_bytes());
        assert_eq!(payload[4], 1, "one locator hash");
        assert_eq!(&payload[5..37], &wire_hash());
        assert_eq!(&payload[37..], &[0u8; 32], "zero stop hash");
        assert_ne!(wire_hash()[0], 0, "the test hash must not be a palindrome");
    })
    .await;
}

#[tokio::test]
async fn one_header_reply_verifies() {
    bounded(async {
        let p = checkpointed();
        let mut s = FakeScript::new(p.magic, 70016);
        s.headers_reply = Some(headers(1, wire_hash(), 0));
        let (res, seen) = run(s, &p, &checking()).await;
        assert_eq!(res.unwrap().chain, ChainCheck::Verified);
        assert!(!seen.iter().any(|c| c == "getaddr"), "{seen:?}");
    })
    .await;
}

#[tokio::test]
async fn mismatched_prev_is_wrong_chain() {
    bounded(async {
        let p = checkpointed();
        let mut other = wire_hash();
        other[0] ^= 0xff;
        let mut s = FakeScript::new(p.magic, 70016);
        s.headers_reply = Some(headers(2000, other, 0));
        let (res, _) = run(s, &p, &checking()).await;
        assert_eq!(res.unwrap().chain, ChainCheck::WrongChain);
    })
    .await;
}

#[tokio::test]
async fn empty_or_missing_headers_is_unverified() {
    bounded(async {
        let p = checkpointed();
        let mut s = FakeScript::new(p.magic, 70016);
        s.headers_reply = Some(headers(0, [0; 32], 0));
        let (res, _) = run(s, &p, &checking()).await;
        assert_eq!(res.unwrap().chain, ChainCheck::Unverified);

        // No reply: the headers wait ends at handshake_timeout (2 s, after the 1 s address
        // grace has already ended collection), and the probe still succeeds.
        let mut s = FakeScript::new(p.magic, 70016);
        s.addr_reply = vec![entry(1), entry(2)];
        let o = ProbeOptions {
            handshake_timeout: Duration::from_secs(2),
            addr_wait: Duration::from_secs(5),
            want_addrs: true,
            ..checking()
        };
        let start = Instant::now();
        let (res, seen) = run(s, &p, &o).await;
        let res = res.unwrap();
        let took = start.elapsed();
        assert_eq!(res.chain, ChainCheck::Unverified);
        assert_eq!(res.addrs, vec![entry(1), entry(2)]);
        assert!(seen.iter().any(|c| c == "getheaders"), "{seen:?}");
        assert!(
            took >= Duration::from_millis(1800) && took < Duration::from_secs(4),
            "{took:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn check_only_probe_waits_handshake_timeout_without_getaddr() {
    bounded(async {
        let p = checkpointed();
        let mut s = FakeScript::new(p.magic, 70016);
        s.addr_reply = vec![entry(1), entry(2)];
        let o = ProbeOptions {
            handshake_timeout: Duration::from_secs(2),
            ..checking()
        };
        let start = Instant::now();
        let (res, seen) = run(s, &p, &o).await;
        let took = start.elapsed();
        let res = res.unwrap();
        assert_eq!(res.chain, ChainCheck::Unverified);
        assert!(res.addrs.is_empty());
        assert!(seen.iter().any(|c| c == "getheaders"), "{seen:?}");
        assert!(!seen.iter().any(|c| c == "getaddr"), "{seen:?}");
        assert!(
            took >= Duration::from_millis(1800) && took < Duration::from_secs(4),
            "{took:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn no_check_at_or_below_the_checkpoint_height_or_when_not_asked() {
    bounded(async {
        let p = checkpointed();
        let mut s = FakeScript::new(p.magic, 70016);
        s.start_height = CHECKPOINT_HEIGHT;
        s.headers_reply = Some(headers(1, wire_hash(), 0));
        let (res, seen) = run(s.clone(), &p, &checking()).await;
        assert_eq!(res.unwrap().chain, ChainCheck::NotChecked);
        assert!(!seen.iter().any(|c| c == "getheaders"), "{seen:?}");

        s.start_height = CHECKPOINT_HEIGHT + 1;
        let (res, seen) = run(s.clone(), &p, &opts()).await;
        assert_eq!(res.unwrap().chain, ChainCheck::NotChecked);
        assert!(!seen.iter().any(|c| c == "getheaders"), "{seen:?}");

        // A preset without a checkpoint never checks.
        let plain = inline("version_trailer = \"none\"");
        let (res, seen) = run(s, &plain, &checking()).await;
        assert_eq!(res.unwrap().chain, ChainCheck::NotChecked);
        assert!(!seen.iter().any(|c| c == "getheaders"), "{seen:?}");
    })
    .await;
}

#[tokio::test]
async fn oversize_headers_reply_is_handled_by_prefix() {
    bounded(async {
        let p = checkpointed();
        let mut s = FakeScript::new(p.magic, 70016);
        s.headers_reply = Some(headers(2000, wire_hash(), 2 << 20));
        let (res, _) = run(s.clone(), &p, &checking()).await;
        assert_eq!(res.unwrap().chain, ChainCheck::Verified);

        // Over the 32 MiB cap: unverified, and the probe still succeeds.
        s.headers_reply = Some(headers(2000, wire_hash(), (32 << 20) + 1));
        let (res, _) = run(s, &p, &checking()).await;
        assert_eq!(res.unwrap().chain, ChainCheck::Unverified);
    })
    .await;
}
