//! A scripted fake peer, shared by the integration tests.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use seedcrystal::coin::{Checksum, VersionTrailer};
use seedcrystal::p2p::{
    AddrEntry, VersionMsg, Wire, encode_addr, encode_addrv2, encode_frame, encode_version,
    read_frame,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout, timeout_at};

/// How long a fake peer lives at most, whatever the client does.
const LIFETIME: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct FakeScript {
    pub magic: [u8; 4],
    pub version: i32,
    pub services: u64,
    pub start_height: i32,
    /// Raw bytes appended to the version payload (Dash sends 33).
    pub trailing: Vec<u8>,
    /// Messages sent between the fake's `version` and its `verack`, as Bitcoin Core does.
    /// Ignored with `drip`.
    pub between_version_and_verack: Vec<(String, Vec<u8>)>,
    /// Extra messages sent right after `verack`.
    pub after_verack: Vec<(String, Vec<u8>)>,
    /// Entries sent in answer to `getaddr`.
    pub addr_reply: Vec<AddrEntry>,
    pub use_addrv2: bool,
    /// Accept the connection and say nothing.
    pub silent: bool,
    /// Send the version message one byte per 200 ms.
    pub drip: bool,
    /// After the handshake, flood `ping`s and never read again (behind a tiny receive buffer).
    pub flood_pings: bool,
    /// Close the connection right after answering `getaddr` (default: keep it open).
    pub close_after_getaddr: bool,
    /// The eCash rule: any message other than `verack` received before the client's `verack`
    /// (its `version` aside) closes the connection. The fake sends its own `verack` only after the client's.
    pub strict_pre_verack: bool,
    /// The PIVX rule: a non-zero 32-byte block after the client's relay byte closes the
    /// connection. The fake sends its own `verack` only after the client's.
    pub reject_nonzero_challenge: bool,
    /// Wait this long after `getaddr` before replying.
    pub addr_delay: Duration,
    /// Sent as one `addr` right after the handshake, before any reply to `getaddr`.
    pub relay_flush: Vec<AddrEntry>,
    pub checksum: Checksum,
    /// Raw `headers` payload sent in answer to `getheaders`; `None` ignores `getheaders`.
    pub headers_reply: Option<Vec<u8>>,
}

impl FakeScript {
    pub fn new(magic: [u8; 4], version: i32) -> Self {
        Self {
            magic,
            version,
            services: 1,
            start_height: 100,
            trailing: Vec::new(),
            between_version_and_verack: Vec::new(),
            after_verack: Vec::new(),
            addr_reply: Vec::new(),
            use_addrv2: false,
            silent: false,
            drip: false,
            flood_pings: false,
            close_after_getaddr: false,
            strict_pre_verack: false,
            reject_nonzero_challenge: false,
            addr_delay: Duration::ZERO,
            relay_flush: Vec::new(),
            checksum: Checksum::Sha256d,
            headers_reply: None,
        }
    }
}

pub struct FakePeer;

pub type Received = Vec<(String, Vec<u8>)>;

impl FakePeer {
    /// Listens on an ephemeral loopback port and serves one connection. The handle yields the
    /// commands the fake received, in order, once the client disconnects.
    pub fn spawn(script: FakeScript) -> (SocketAddr, JoinHandle<Vec<String>>) {
        let (addr, h) = Self::spawn_frames(script);
        let h = tokio::spawn(async move { h.await.unwrap().into_iter().map(|(c, _)| c).collect() });
        (addr, h)
    }

    /// Like [`FakePeer::spawn`], but also yields each received payload.
    pub fn spawn_frames(script: FakeScript) -> (SocketAddr, JoinHandle<Received>) {
        let socket = TcpSocket::new_v4().unwrap();
        if script.flood_pings {
            socket.set_recv_buffer_size(2048).unwrap();
        }
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = socket.local_addr().unwrap();
        let listener = socket.listen(1).unwrap();
        let handle = tokio::spawn(async move {
            let deadline = Instant::now() + LIFETIME;
            let Ok(Ok((stream, _))) = timeout_at(deadline, listener.accept()).await else {
                return Vec::new();
            };
            serve(stream, script, deadline).await
        });
        (addr, handle)
    }
}

async fn serve(stream: tokio::net::TcpStream, script: FakeScript, deadline: Instant) -> Received {
    let (mut rd, mut wr) = stream.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Vec<u8>)>();
    let wire = Wire {
        magic: script.magic,
        checksum: script.checksum,
    };
    let reader = tokio::spawn(async move {
        while let Ok(frame) = read_frame(&mut rd, wire).await {
            if tx.send((frame.command, frame.payload)).is_err() {
                break;
            }
        }
    });

    let mut seen: Received = Vec::new();
    if !script.silent {
        let mut version = encode_version(
            &VersionMsg {
                version: script.version,
                services: script.services,
                timestamp: 1_700_000_000,
                receiver: "127.0.0.1:1".parse().unwrap(),
                nonce: 7,
                user_agent: "/fake:1/".into(),
                start_height: script.start_height,
                relay: true,
            },
            VersionTrailer::None,
        );
        version.extend_from_slice(&script.trailing);
        let version_frame = encode_frame(wire, "version", &version);

        if script.drip {
            for byte in version_frame {
                if wr.write_all(&[byte]).await.is_err() {
                    return seen;
                }
                tokio::select! {
                    _ = sleep(Duration::from_millis(200)) => {}
                    cmd = rx.recv() => match cmd {
                        Some(c) => seen.push(c),
                        None => return seen,
                    },
                }
            }
        } else {
            let gated = script.strict_pre_verack || script.reject_nonzero_challenge;
            let mut out = version_frame;
            if gated {
                if wr.write_all(&out).await.is_err() {
                    return seen;
                }
                out = Vec::new();
                loop {
                    let Ok(Some(cmd)) = timeout_at(deadline, rx.recv()).await else {
                        return seen;
                    };
                    let name = cmd.0.clone();
                    let bad = (script.reject_nonzero_challenge
                        && name == "version"
                        && challenge_is_nonzero(&cmd.1))
                        || (script.strict_pre_verack && name != "version" && name != "verack");
                    seen.push(cmd);
                    if bad {
                        let _ = wr.shutdown().await;
                        reader.abort();
                        return seen;
                    }
                    if name == "verack" {
                        break;
                    }
                }
            }
            for (cmd, payload) in &script.between_version_and_verack {
                out.extend(encode_frame(wire, cmd, payload));
            }
            out.extend(encode_frame(wire, "verack", &[]));
            for (cmd, payload) in &script.after_verack {
                out.extend(encode_frame(wire, cmd, payload));
            }
            if !script.relay_flush.is_empty() {
                out.extend(encode_frame(
                    wire,
                    "addr",
                    &encode_addr(&script.relay_flush),
                ));
            }
            if wr.write_all(&out).await.is_err() {
                return seen;
            }
        }
    }

    if script.flood_pings {
        reader.abort();
        let ping = encode_frame(wire, "ping", &0xfeedu64.to_le_bytes());
        loop {
            match timeout(Duration::from_secs(5), wr.write_all(&ping)).await {
                Ok(Ok(())) => {}
                _ => return seen,
            }
        }
    }

    while let Ok(Some(cmd)) = timeout_at(deadline, rx.recv()).await {
        let is_getaddr = cmd.0 == "getaddr";
        let is_getheaders = cmd.0 == "getheaders";
        seen.push(cmd);
        if is_getheaders && let Some(payload) = &script.headers_reply {
            let _ = wr.write_all(&encode_frame(wire, "headers", payload)).await;
        }
        if is_getaddr && !script.addr_reply.is_empty() {
            sleep(script.addr_delay).await;
            let frame = if script.use_addrv2 {
                encode_frame(wire, "addrv2", &encode_addrv2(&script.addr_reply))
            } else {
                encode_frame(wire, "addr", &encode_addr(&script.addr_reply))
            };
            let _ = wr.write_all(&frame).await;
        }
        if is_getaddr && script.close_after_getaddr {
            let _ = wr.shutdown().await;
            reader.abort();
            break;
        }
    }
    seen
}
pub mod fake_cloudflare;
pub mod fake_resolver;

/// True when the bytes after the client's relay byte begin with a non-zero 32-byte block.
fn challenge_is_nonzero(version_payload: &[u8]) -> bool {
    // Fixed fields end at 80; then a one-byte user-agent length, the agent, height and relay.
    let Some(&ua_len) = version_payload.get(80) else {
        return false;
    };
    let trailer = version_payload
        .get(80 + 1 + ua_len as usize + 4 + 1..)
        .unwrap_or(&[]);
    trailer.len() >= 32 && trailer[..32].iter().any(|&b| b != 0)
}
