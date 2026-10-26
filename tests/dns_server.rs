use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hickory_proto::op::{Edns, Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{Name, RecordType};
use seedcrystal::crawler::Snapshot;
use seedcrystal::dns::{Zone, ZoneSet, serve};
use seedcrystal::store::Servable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const WAIT: Duration = Duration::from_secs(5);
const APEX: &str = "dnsseed.osmium.space.";

struct Harness {
    addr: SocketAddr,
    stop: CancellationToken,
    task: JoinHandle<anyhow::Result<()>>,
    _tx: watch::Sender<Arc<Snapshot>>,
}

async fn free_port() -> SocketAddr {
    // Bind-then-drop: pick a port that is free for both UDP and TCP.
    loop {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();
        if UdpSocket::bind(addr).await.is_ok() {
            return addr;
        }
    }
}

async fn start(nodes: Vec<Servable>) -> Harness {
    let (tx, rx) = watch::channel(Arc::new(Snapshot {
        nodes,
        built_at: 1_000_000,
    }));
    let zones = Arc::new(ZoneSet::new(vec![Zone {
        apex: Name::from_ascii(APEX).unwrap(),
        nameserver: Name::from_ascii("ns.dnsseed.osmium.space.").unwrap(),
        mailbox: Name::from_ascii("hostmaster.dnsseed.osmium.space.").unwrap(),
        seed_flags: 1,
        snapshot: rx,
    }]));
    let addr = free_port().await;
    let stop = CancellationToken::new();
    let task = tokio::spawn(serve(zones, vec![addr], stop.clone()));
    // Wait until the TCP listener accepts.
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Harness {
        addr,
        stop,
        task,
        _tx: tx,
    }
}

fn v4(i: u8) -> Servable {
    Servable {
        addr: format!("203.{i}.113.1:23222").parse().unwrap(),
        services: 1,
    }
}

fn v6(i: u16) -> Servable {
    Servable {
        addr: format!("[2001:{i:x}::1]:23222").parse().unwrap(),
        services: 1,
    }
}

fn query(name: &str, t: RecordType, edns: bool) -> Message {
    let mut m = Message::new(0x1234, MessageType::Query, OpCode::Query);
    m.metadata.recursion_desired = true;
    m.add_query(Query::query(Name::from_ascii(name).unwrap(), t));
    if edns {
        let mut e = Edns::new();
        e.set_max_payload(1232);
        m.set_edns(e);
    }
    m
}

async fn udp_exchange(addr: SocketAddr, wire: &[u8]) -> Vec<u8> {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sock.send_to(wire, addr).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let (n, _) = timeout(WAIT, sock.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    buf.truncate(n);
    buf
}

async fn tcp_exchange(addr: SocketAddr, wire: &[u8]) -> Vec<u8> {
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(&(wire.len() as u16).to_be_bytes())
        .await
        .unwrap();
    s.write_all(wire).await.unwrap();
    let mut len = [0u8; 2];
    timeout(WAIT, s.read_exact(&mut len))
        .await
        .unwrap()
        .unwrap();
    let mut body = vec![0u8; u16::from_be_bytes(len) as usize];
    timeout(WAIT, s.read_exact(&mut body))
        .await
        .unwrap()
        .unwrap();
    body
}

#[tokio::test]
async fn udp_a_query_returns_addresses() {
    let h = start(vec![v4(1), v4(2), v4(3)]).await;
    let q = query("X1.DnSsEeD.OsMiUm.SpAcE.", RecordType::A, true);
    let resp = Message::from_vec(&udp_exchange(h.addr, &q.to_vec().unwrap()).await).unwrap();
    assert_eq!(resp.metadata.id, 0x1234);
    assert_eq!(resp.metadata.message_type, MessageType::Response);
    assert!(resp.metadata.authoritative);
    assert!(resp.metadata.recursion_desired);
    assert_eq!(resp.metadata.response_code, ResponseCode::NoError);
    assert_eq!(resp.answers.len(), 3);
    assert_eq!(
        resp.queries[0].name().to_ascii(),
        "X1.DnSsEeD.OsMiUm.SpAcE."
    );
    assert!(resp.edns.is_some());
    h.stop.cancel();
}

#[tokio::test]
async fn tcp_query_returns_same_records() {
    let h = start(vec![v4(1), v4(2), v4(3)]).await;
    let q = query("x1.dnsseed.osmium.space.", RecordType::A, false);
    let wire = q.to_vec().unwrap();
    let t = Message::from_vec(&tcp_exchange(h.addr, &wire).await).unwrap();
    let u = Message::from_vec(&udp_exchange(h.addr, &wire).await).unwrap();
    let mut ta: Vec<_> = t.answers.iter().map(|r| r.data.to_string()).collect();
    let mut ua: Vec<_> = u.answers.iter().map(|r| r.data.to_string()).collect();
    ta.sort();
    ua.sort();
    assert_eq!(ta.len(), 3);
    assert_eq!(ta, ua);
    h.stop.cancel();
}

#[tokio::test]
async fn no_edns_response_fits_512_bytes() {
    let h = start((1..=40).map(v6).collect()).await;
    let q = query("x1.dnsseed.osmium.space.", RecordType::AAAA, false);
    let raw = udp_exchange(h.addr, &q.to_vec().unwrap()).await;
    assert!(raw.len() <= 512, "{} bytes", raw.len());
    let resp = Message::from_vec(&raw).unwrap();
    assert!(!resp.metadata.truncation);
    assert_eq!(resp.answers.len(), 15);
    h.stop.cancel();
}

#[tokio::test]
async fn edns_response_is_shed_to_the_clamped_payload_budget() {
    let h = start((1..=40).map(v6).collect()).await;
    // 256 is below the 512 floor, so both payloads get the same 512-byte budget.
    for payload in [512u16, 256] {
        let mut q = query("x1.dnsseed.osmium.space.", RecordType::AAAA, true);
        let mut e = Edns::new();
        e.set_max_payload(payload);
        q.set_edns(e);
        let raw = udp_exchange(h.addr, &q.to_vec().unwrap()).await;
        assert!(raw.len() <= 512, "payload {payload}: {} bytes", raw.len());
        // Without the clamp the 256 case would shed to at most 256 bytes.
        assert!(raw.len() > 256, "payload {payload}: {} bytes", raw.len());
        let resp = Message::from_vec(&raw).unwrap();
        assert!(!resp.metadata.truncation, "payload {payload}");
        assert!(
            (8..=19).contains(&resp.answers.len()),
            "payload {payload}: {} answers",
            resp.answers.len()
        );
    }
    h.stop.cancel();
}

#[tokio::test]
async fn empty_zone_and_foreign_names_answer_over_the_wire() {
    let h = start(vec![]).await;
    let q = query("x1.dnsseed.osmium.space.", RecordType::A, false);
    let r = Message::from_vec(&udp_exchange(h.addr, &q.to_vec().unwrap()).await).unwrap();
    assert_eq!(r.metadata.response_code, ResponseCode::NoError);
    assert!(r.answers.is_empty());
    assert_eq!(r.authorities.len(), 1);
    let q = query("example.com.", RecordType::A, false);
    let r = Message::from_vec(&udp_exchange(h.addr, &q.to_vec().unwrap()).await).unwrap();
    assert_eq!(r.metadata.response_code, ResponseCode::Refused);
    h.stop.cancel();
}

#[tokio::test]
async fn garbage_is_survived_and_server_keeps_answering() {
    let h = start(vec![v4(1)]).await;
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sock.send_to(&[0xde, 0xad], h.addr).await.unwrap();
    sock.send_to(&[0u8; 40], h.addr).await.unwrap();
    // Header claiming a question that is not there: FORMERR or silence, no crash.
    let mut bad = vec![0x12, 0x34, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    bad.extend_from_slice(&[63, b'a']);
    sock.send_to(&bad, h.addr).await.unwrap();
    let q = query("x1.dnsseed.osmium.space.", RecordType::A, false);
    let r = Message::from_vec(&udp_exchange(h.addr, &q.to_vec().unwrap()).await).unwrap();
    assert_eq!(r.answers.len(), 1);
    h.stop.cancel();
}

#[tokio::test]
async fn tcp_idle_connection_is_closed() {
    let h = start(vec![v4(1)]).await;
    let mut s = TcpStream::connect(h.addr).await.unwrap();
    let mut buf = [0u8; 1];
    let n = timeout(Duration::from_secs(15), s.read(&mut buf))
        .await
        .expect("server must close an idle connection")
        .unwrap_or(0);
    assert_eq!(n, 0);
    h.stop.cancel();
}

#[tokio::test]
async fn server_stops_on_shutdown() {
    let h = start(vec![v4(1)]).await;
    // An open idle TCP connection must not delay shutdown.
    let _idle = TcpStream::connect(h.addr).await.unwrap();
    h.stop.cancel();
    let res = timeout(WAIT, h.task).await.expect("serve returns").unwrap();
    assert!(res.is_ok());
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sock.send_to(&query(APEX, RecordType::A, false).to_vec().unwrap(), h.addr)
        .await
        .unwrap();
    let mut buf = [0u8; 512];
    assert!(
        timeout(Duration::from_millis(500), sock.recv_from(&mut buf))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn tcp_client_that_never_reads_is_dropped_and_service_continues() {
    let h = start(vec![v4(1)]).await;
    let sock = tokio::net::TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(1024).unwrap();
    let mut s = sock.connect(h.addr).await.unwrap();
    let wire = query("x1.dnsseed.osmium.space.", RecordType::A, true)
        .to_vec()
        .unwrap();
    let mut frame = (wire.len() as u16).to_be_bytes().to_vec();
    frame.extend_from_slice(&wire);
    for _ in 0..200 {
        // Writes may fail once the server has closed; that is the point.
        if s.write_all(&frame).await.is_err() {
            break;
        }
    }
    // Never read; the server must still close within the cap plus slack.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let r = Message::from_vec(&tcp_exchange(h.addr, &wire).await).unwrap();
    assert_eq!(r.answers.len(), 1);
    let mut sink = Vec::new();
    timeout(Duration::from_secs(25), s.read_to_end(&mut sink))
        .await
        .expect("server closes the non-reading connection")
        .ok();
    h.stop.cancel();
}

#[tokio::test]
async fn axfr_is_refused_over_tcp() {
    let h = start(vec![v4(1)]).await;
    let q = query(APEX, RecordType::AXFR, false);
    let r = Message::from_vec(&tcp_exchange(h.addr, &q.to_vec().unwrap()).await).unwrap();
    assert_eq!(r.metadata.response_code, ResponseCode::Refused);
    h.stop.cancel();
}

#[tokio::test]
async fn wildcard_v4_and_v6_listeners_share_one_port() {
    if UdpSocket::bind("[::1]:0").await.is_err() {
        eprintln!("SKIPPED: this host has no IPv6 loopback");
        return;
    }
    let (_tx, rx) = watch::channel(Arc::new(Snapshot {
        nodes: vec![v4(1)],
        built_at: 1_000_000,
    }));
    let zones = Arc::new(ZoneSet::new(vec![Zone {
        apex: Name::from_ascii(APEX).unwrap(),
        nameserver: Name::from_ascii("ns.osmium.space.").unwrap(),
        mailbox: Name::from_ascii("hostmaster.osmium.space.").unwrap(),
        seed_flags: 1,
        snapshot: rx,
    }]));
    let port = free_port().await.port();
    let (a4, a6): (SocketAddr, SocketAddr) = (
        format!("127.0.0.1:{port}").parse().unwrap(),
        format!("[::1]:{port}").parse().unwrap(),
    );
    // The wildcard pair is the default config; it collides unless the v6 socket is v6-only.
    let wild = vec![
        format!("0.0.0.0:{port}").parse().unwrap(),
        format!("[::]:{port}").parse().unwrap(),
    ];
    let stop = CancellationToken::new();
    let task = tokio::spawn(serve(zones, wild, stop.clone()));
    let wire = query("x1.dnsseed.osmium.space.", RecordType::A, false)
        .to_vec()
        .unwrap();
    for addr in [a4, a6] {
        for _ in 0..50 {
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let local = if addr.is_ipv6() {
            "[::1]:0"
        } else {
            "127.0.0.1:0"
        };
        let s = UdpSocket::bind(local).await.unwrap();
        s.send_to(&wire, addr).await.unwrap();
        let mut buf = vec![0u8; 1500];
        let n = timeout(WAIT, s.recv(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("no UDP answer on {addr} (did serve fail to bind both?)"))
            .unwrap();
        let resp = Message::from_vec(&buf[..n]).unwrap();
        assert_eq!(resp.answers.len(), 1, "{addr}");
    }
    stop.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn server_counts_answered_and_empty_but_not_nxdomain_or_refused() {
    use seedcrystal::dns::bind;
    use seedcrystal::dns::querylog::{Counters, QueryLogs};

    let (_tx, rx) = watch::channel(Arc::new(Snapshot {
        nodes: vec![v4(1)],
        built_at: 1_000_000,
    }));
    let zones = Arc::new(ZoneSet::new(vec![Zone {
        apex: Name::from_ascii(APEX).unwrap(),
        nameserver: Name::from_ascii("ns.dnsseed.osmium.space.").unwrap(),
        mailbox: Name::from_ascii("hostmaster.dnsseed.osmium.space.").unwrap(),
        seed_flags: 1,
        snapshot: rx,
    }]));
    let counters = Arc::new(Counters::new(1));
    let mut logs = QueryLogs::new();
    logs.add(Name::from_ascii(APEX).unwrap(), counters.clone());
    let addr = free_port().await;
    let stop = CancellationToken::new();
    let task = tokio::spawn(bind(&[addr]).unwrap().run_logged(
        zones,
        Some(Arc::new(logs)),
        stop.clone(),
    ));

    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for (name, t) in [
        ("x1.dnsseed.osmium.space.", RecordType::A),
        ("X1.dnsseed.osmium.space.", RecordType::A),
        ("x1.dnsseed.osmium.space.", RecordType::AAAA),
        ("junk.dnsseed.osmium.space.", RecordType::A),
        ("elsewhere.example.", RecordType::A),
    ] {
        sock.send_to(&query(name, t, false).to_vec().unwrap(), addr)
            .await
            .unwrap();
        let mut buf = [0u8; 1500];
        timeout(WAIT, sock.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
    }
    let log = counters.snapshot();
    stop.cancel();
    task.await.unwrap().unwrap();
    assert_eq!(log.names.len(), 1, "{log:?}");
    let s = &log.names["x1.dnsseed.osmium.space"];
    assert_eq!((s.answered, s.empty), (2, 1), "{log:?}");
}
