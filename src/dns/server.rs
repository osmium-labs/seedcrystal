//! UDP and TCP front ends. We run our own loops over hickory-proto's wire
//! codec rather than a server framework: the answer is a hand-built
//! `Message` (authoritative flag, exact question echo, SOA in authority) and
//! the size budget for UDP needs control over which records are dropped.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use hickory_proto::op::{Message, MessageType, OpCode, ResponseCode};
use hickory_proto::rr::RecordType;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::answer::{ADVERTISED_PAYLOAD, ZoneSet, build_answer};
use super::querylog::QueryLogs;

const TCP_IDLE: Duration = Duration::from_secs(10);
const MAX_TCP_CONNECTIONS: usize = 512;
const MAX_QUERIES_PER_CONNECTION: usize = 100;
const UDP_NO_EDNS_LIMIT: usize = 512;
const UDP_RECV_BUF: usize = 4096;

/// Sockets bound ahead of serving, so a bind failure surfaces before anything
/// else is started.
pub struct Bound {
    udp: Vec<Arc<UdpSocket>>,
    tcp: Vec<TcpListener>,
}

/// Bind UDP and TCP at every address in `listen`.
pub fn bind(listen: &[SocketAddr]) -> Result<Bound> {
    let mut udp = Vec::new();
    let mut tcp = Vec::new();
    for addr in listen {
        udp.push(Arc::new(
            bind_udp(*addr).with_context(|| format!("bind udp {addr}"))?,
        ));
        tcp.push(bind_tcp(*addr).with_context(|| format!("bind tcp {addr}"))?);
    }
    Ok(Bound { udp, tcp })
}

impl Bound {
    /// Answer queries for `zones` until `shutdown` is cancelled.
    pub async fn run(self, zones: Arc<ZoneSet>, shutdown: CancellationToken) -> Result<()> {
        self.run_logged(zones, None, shutdown).await
    }

    /// Like [`Bound::run`], counting the queries it answers in `logs`.
    pub async fn run_logged(
        self,
        zones: Arc<ZoneSet>,
        logs: Option<Arc<QueryLogs>>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let permits = Arc::new(Semaphore::new(MAX_TCP_CONNECTIONS));
        let mut tasks = JoinSet::new();
        for sock in self.udp {
            tasks.spawn(udp_loop(sock, zones.clone(), logs.clone()));
        }
        for listener in self.tcp {
            tasks.spawn(tcp_loop(
                listener,
                zones.clone(),
                logs.clone(),
                permits.clone(),
            ));
        }
        shutdown.cancelled().await;
        // Dropping the set aborts the loops and every connection task under them.
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
}

/// Answer queries for `zones` on UDP and TCP at every address in `listen`
/// until `shutdown` is cancelled. Bind failures are returned immediately.
pub async fn serve(
    zones: Arc<ZoneSet>,
    listen: Vec<SocketAddr>,
    shutdown: CancellationToken,
) -> Result<()> {
    bind(&listen)?.run(zones, shutdown).await
}

/// IPv6 sockets are v6-only, so `[::]:53` and `0.0.0.0:53` coexist whatever
/// `net.ipv6.bindv6only` says.
fn new_socket(addr: SocketAddr, ty: socket2::Type) -> std::io::Result<socket2::Socket> {
    let sock = socket2::Socket::new(socket2::Domain::for_address(addr), ty, None)?;
    if addr.is_ipv6() {
        sock.set_only_v6(true)?;
    }
    sock.set_nonblocking(true)?;
    Ok(sock)
}

fn bind_udp(addr: SocketAddr) -> std::io::Result<UdpSocket> {
    let sock = new_socket(addr, socket2::Type::DGRAM)?;
    sock.bind(&addr.into())?;
    UdpSocket::from_std(sock.into())
}

fn bind_tcp(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let sock = new_socket(addr, socket2::Type::STREAM)?;
    sock.set_reuse_address(true)?;
    sock.bind(&addr.into())?;
    sock.listen(1024)?;
    TcpListener::from_std(sock.into())
}

async fn udp_loop(sock: Arc<UdpSocket>, zones: Arc<ZoneSet>, logs: Option<Arc<QueryLogs>>) {
    let mut buf = vec![0u8; UDP_RECV_BUF];
    loop {
        let (n, peer) = match sock.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                // e.g. ICMP port-unreachable surfacing on a previous send.
                debug!("udp recv: {e}");
                continue;
            }
        };
        if let Some(out) = respond(&zones, logs.as_deref(), &buf[..n], true)
            && let Err(e) = sock.send_to(&out, peer).await
        {
            debug!("udp send to {peer}: {e}");
        }
    }
}

async fn tcp_loop(
    listener: TcpListener,
    zones: Arc<ZoneSet>,
    logs: Option<Arc<QueryLogs>>,
    permits: Arc<Semaphore>,
) {
    let mut conns = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    let Ok(permit) = permits.clone().try_acquire_owned() else {
                        debug!("tcp {peer}: connection limit reached");
                        continue;
                    };
                    let zones = zones.clone();
                    let logs = logs.clone();
                    conns.spawn(async move {
                        let _permit = permit;
                        if let Err(e) = tcp_conn(stream, zones, logs, TCP_IDLE).await {
                            debug!("tcp {peer}: {e}");
                        }
                    });
                }
                Err(e) => {
                    warn!("tcp accept: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
}

async fn tcp_conn<S>(
    mut stream: S,
    zones: Arc<ZoneSet>,
    logs: Option<Arc<QueryLogs>>,
    idle: Duration,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    for _ in 0..MAX_QUERIES_PER_CONNECTION {
        let mut len = [0u8; 2];
        match timeout(idle, stream.read_exact(&mut len)).await {
            Ok(Ok(_)) => {}
            // Idle too long, or the peer closed.
            _ => return Ok(()),
        }
        let len = usize::from(u16::from_be_bytes(len));
        if len == 0 {
            return Ok(());
        }
        let mut body = vec![0u8; len];
        timeout(idle, stream.read_exact(&mut body))
            .await
            .context("timed out reading query body")??;
        if let Some(out) = respond(&zones, logs.as_deref(), &body, false) {
            let frame_len = u16::try_from(out.len()).context("response over 64 KiB")?;
            let mut frame = Vec::with_capacity(out.len() + 2);
            frame.extend_from_slice(&frame_len.to_be_bytes());
            frame.extend_from_slice(&out);
            // A peer that never reads must not hold the connection forever.
            timeout(idle, stream.write_all(&frame))
                .await
                .context("timed out writing response")??;
        }
    }
    Ok(())
}

/// Decode one query datagram/frame and produce the wire response, or `None`
/// when the packet should be silently dropped.
fn respond(zones: &ZoneSet, logs: Option<&QueryLogs>, wire: &[u8], udp: bool) -> Option<Vec<u8>> {
    // Never answer a response or something too short to carry an id.
    if wire.len() < 12 || wire[2] & 0x80 != 0 {
        return None;
    }
    let id = u16::from_be_bytes([wire[0], wire[1]]);
    let Ok(req) = Message::from_vec(wire) else {
        return encode(&Message::error_msg(
            id,
            OpCode::Query,
            ResponseCode::FormErr,
        ));
    };
    if req.metadata.message_type != MessageType::Query {
        return None;
    }
    if req.metadata.op_code != OpCode::Query {
        return encode(&Message::error_msg(
            id,
            req.metadata.op_code,
            ResponseCode::NotImp,
        ));
    }
    if req.queries.len() != 1 {
        return encode(&Message::error_msg(
            id,
            OpCode::Query,
            ResponseCode::FormErr,
        ));
    }
    let edns_payload = req.edns.as_ref().map(|e| e.max_payload());
    let mut resp = build_answer(
        zones,
        &req.queries[0],
        edns_payload.is_some(),
        &mut rand::rng(),
    );
    if let Some(logs) = logs
        && resp.metadata.response_code == ResponseCode::NoError
        && matches!(
            req.queries[0].query_type(),
            RecordType::A | RecordType::AAAA
        )
    {
        let answered = resp.answers.iter().any(|r| r.data.ip_addr().is_some());
        logs.record(req.queries[0].name(), answered);
    }
    resp.metadata.id = id;
    resp.metadata.recursion_desired = req.metadata.recursion_desired;
    resp.metadata.checking_disabled = req.metadata.checking_disabled;

    let mut out = resp.to_vec().ok()?;
    if udp {
        let limit = match edns_payload {
            // hickory already raises an advertised size below 512 to 512 (RFC 6891).
            Some(p) => usize::from(p.min(ADVERTISED_PAYLOAD)),
            None => UDP_NO_EDNS_LIMIT,
        };
        // A random subset is as good as another: shed answers to fit.
        while out.len() > limit && resp.answers.pop().is_some() {
            out = resp.to_vec().ok()?;
        }
        if out.len() > limit {
            resp.metadata.truncation = true;
            out = resp.to_vec().ok()?;
        }
    }
    Some(out)
}

fn encode(msg: &Message) -> Option<Vec<u8>> {
    msg.to_vec().ok()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use hickory_proto::op::Query;
    use hickory_proto::rr::Name;
    use tokio::sync::watch;

    use super::*;
    use crate::crawler::Snapshot;
    use crate::dns::answer::Zone;

    #[tokio::test]
    async fn tcp_conn_gives_up_on_a_peer_that_never_reads() {
        let (_tx, rx) = watch::channel(Arc::new(Snapshot::default()));
        let zones = Arc::new(ZoneSet::new(vec![Zone {
            apex: Name::from_str("seed.example.").unwrap(),
            nameserver: Name::from_str("ns.seed.example.").unwrap(),
            mailbox: Name::from_str("h.seed.example.").unwrap(),
            seed_flags: 1,
            snapshot: rx,
        }]));
        // 64-byte pipe nobody drains: the first response cannot be written.
        let (mut client, server) = tokio::io::duplex(64);
        let task = tokio::spawn(tcp_conn(server, zones, None, Duration::from_millis(200)));
        let mut q = Message::new(1, MessageType::Query, OpCode::Query);
        q.add_query(Query::query(
            Name::from_str("seed.example.").unwrap(),
            RecordType::SOA,
        ));
        let wire = q.to_vec().unwrap();
        for _ in 0..3 {
            let _ = client.write_all(&(wire.len() as u16).to_be_bytes()).await;
            let _ = client.write_all(&wire).await;
        }
        let res = timeout(Duration::from_secs(5), task)
            .await
            .expect("tcp_conn must return")
            .unwrap();
        assert!(res.is_err(), "write timeout surfaces as an error");
    }
}
