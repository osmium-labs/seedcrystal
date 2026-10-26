//! One-shot peer probe: connect, handshake by the preset's rules, optionally prove the chain
//! against a checkpoint and harvest addresses.

use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout, timeout_at};

use super::{
    AddrEntry, VersionMsg, WireError, decode_addr, decode_addrv2, decode_headers_first_prev,
    decode_ping, decode_version, encode_frame, encode_getheaders, encode_ping, encode_version,
    read_frame, read_frame_tolerant,
};
use crate::coin::{AddrV2Placement, Checkpoint, CoinPreset};

/// Entries collected from one peer before we stop listening.
const MAX_HARVEST: usize = 1000;

/// How long collection continues after the first multi-entry address message.
const GRACE: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct ProbeOptions {
    pub connect_timeout: Duration,
    /// Covers the whole version/verack exchange, not each read.
    pub handshake_timeout: Duration,
    pub want_addrs: bool,
    /// Covers the whole address-collection phase.
    pub addr_wait: Duration,
    /// Prove the peer's chain against the preset's checkpoint (see [`ChainCheck`]).
    pub check_chain: bool,
}

impl Default for ProbeOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(10),
            want_addrs: false,
            addr_wait: Duration::from_secs(30),
            check_chain: false,
        }
    }
}

/// What the checkpoint probe proved about the peer's chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainCheck {
    /// Not asked, no checkpoint in the preset, or the peer is at or below its height.
    NotChecked,
    /// The first header after the checkpoint builds on it.
    Verified,
    /// The peer has headers after the checkpoint, but not on top of it.
    WrongChain,
    /// Asked, but no usable answer: empty reply, timeout, malformed or oversize frame.
    Unverified,
}

#[derive(Debug)]
pub struct ProbeResult {
    pub peer: VersionMsg,
    pub addrs: Vec<AddrEntry>,
    pub chain: ChainCheck,
}

#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    #[error("connect failed: {0}")]
    Connect(std::io::Error),
    #[error("timed out")]
    Timeout,
    #[error("peer protocol version {0} is below the floor")]
    VersionTooLow(i32),
    #[error("wire error: {0}")]
    Wire(WireError),
    #[error("peer closed the connection")]
    Closed,
}

impl From<WireError> for ProbeError {
    fn from(e: WireError) -> Self {
        match e {
            WireError::Truncated => ProbeError::Closed,
            WireError::Io(ref io) if is_closed(io) => ProbeError::Closed,
            other => ProbeError::Wire(other),
        }
    }
}

impl From<std::io::Error> for ProbeError {
    fn from(e: std::io::Error) -> Self {
        WireError::Io(e).into()
    }
}

/// A peer that hangs up mid-exchange surfaces as EOF on reads and as a reset or broken pipe
/// on writes; all of them mean it closed the connection.
fn is_closed(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::{BrokenPipe, ConnectionAborted, ConnectionReset, UnexpectedEof};
    matches!(
        e.kind(),
        UnexpectedEof | BrokenPipe | ConnectionReset | ConnectionAborted
    )
}

/// Connects to `target`, completes the handshake and, if asked, collects `addr`/`addrv2` entries.
pub async fn probe(
    target: SocketAddr,
    preset: &CoinPreset,
    opts: &ProbeOptions,
) -> Result<ProbeResult, ProbeError> {
    let mut stream = match timeout(opts.connect_timeout, TcpStream::connect(target)).await {
        Err(_) => return Err(ProbeError::Timeout),
        Ok(Err(e)) => return Err(ProbeError::Connect(e)),
        Ok(Ok(s)) => s,
    };
    let _ = stream.set_nodelay(true);

    let deadline = Instant::now() + opts.handshake_timeout;
    let peer = timeout_at(deadline, handshake(&mut stream, target, preset))
        .await
        .map_err(|_| ProbeError::Timeout)??;

    let checkpoint = preset
        .checkpoint
        .as_ref()
        .filter(|c| opts.check_chain && peer.start_height > c.height);
    let mut after = AfterHandshake::default();
    if checkpoint.is_some() || opts.want_addrs {
        after_handshake(&mut stream, preset, opts, checkpoint, &mut after).await;
    }
    Ok(ProbeResult {
        peer,
        addrs: after.addrs,
        chain: after.chain,
    })
}

async fn send<W: AsyncWrite + Unpin>(
    w: &mut W,
    preset: &CoinPreset,
    command: &str,
    payload: &[u8],
) -> Result<(), ProbeError> {
    w.write_all(&encode_frame(preset.wire(), command, payload))
        .await?;
    Ok(())
}

fn our_version(target: SocketAddr, preset: &CoinPreset) -> VersionMsg {
    VersionMsg {
        version: preset.protocol_version,
        services: 0,
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
        receiver: target,
        nonce: rand::random(),
        user_agent: format!("/seedcrystal:{}/", env!("CARGO_PKG_VERSION")),
        start_height: 0,
        relay: false,
    }
}

async fn handshake(
    stream: &mut TcpStream,
    target: SocketAddr,
    preset: &CoinPreset,
) -> Result<VersionMsg, ProbeError> {
    let ours = our_version(target, preset);
    send(
        stream,
        preset,
        "version",
        &encode_version(&ours, preset.version_trailer),
    )
    .await?;

    let mut peer: Option<VersionMsg> = None;
    let mut got_verack = false;
    loop {
        let frame = read_frame(stream, preset.wire()).await?;
        match frame.command.as_str() {
            "version" if peer.is_none() => {
                let theirs = decode_version(&frame.payload)?;
                if theirs.version < preset.min_peer_version {
                    return Err(ProbeError::VersionTooLow(theirs.version));
                }
                let addrv2 = preset
                    .addrv2_min_version
                    .is_some_and(|min| ours.version.min(theirs.version) >= min);
                // BIP155 puts sendaddrv2 between their version and our verack; some nodes
                // treat any other message before verack as misbehaviour.
                if addrv2 && preset.addrv2_placement == AddrV2Placement::BeforeVerack {
                    send(stream, preset, "sendaddrv2", &[]).await?;
                }
                send(stream, preset, "verack", &[]).await?;
                if addrv2 && preset.addrv2_placement == AddrV2Placement::AfterVerack {
                    send(stream, preset, "sendaddrv2", &[]).await?;
                }
                peer = Some(theirs);
            }
            "verack" => got_verack = true,
            "ping" => reply_pong(stream, preset, &frame.payload).await?,
            _ => {}
        }
        if got_verack && let Some(p) = peer {
            return Ok(p);
        }
    }
}

async fn reply_pong(
    stream: &mut TcpStream,
    preset: &CoinPreset,
    payload: &[u8],
) -> Result<(), ProbeError> {
    // A malformed ping is not worth failing a probe over.
    if let Ok(nonce) = decode_ping(payload) {
        send(stream, preset, "pong", &encode_ping(nonce)).await?;
    }
    Ok(())
}

/// What the post-handshake phase gathered; kept outside its future so a timeout keeps it.
struct AfterHandshake {
    addrs: Vec<AddrEntry>,
    chain: ChainCheck,
}

impl Default for AfterHandshake {
    fn default() -> Self {
        Self {
            addrs: Vec::new(),
            chain: ChainCheck::NotChecked,
        }
    }
}

/// The verdict of a `headers` reply, from its count and first `hashPrevBlock`.
fn chain_verdict(payload: &[u8], checkpoint: &Checkpoint) -> ChainCheck {
    match decode_headers_first_prev(payload) {
        Ok(Some(prev)) if prev == checkpoint.hash => ChainCheck::Verified,
        Ok(Some(_)) => ChainCheck::WrongChain,
        Ok(None) | Err(_) => ChainCheck::Unverified,
    }
}

/// Sends `getheaders` (with `checkpoint`) and then `getaddr` (with `want_addrs`), and reads
/// both replies in one loop until each is settled or past its deadline.
///
/// The `headers` wait ends `handshake_timeout` after the handshake; only its first header's
/// `hashPrevBlock` is read, and an oversize reply is checked through the prefix that
/// `read_frame_tolerant` keeps. Address collection gathers entries until `addr_wait` elapses or
/// [`MAX_HARVEST`] entries arrive. Older nodes answer after a random delay and may first flush
/// a one-entry relay `addr`, so only a multi-entry message shortens that wait: collection then
/// ends [`GRACE`] later. Reads and writes share the latest pending deadline, so a peer that never drains our
/// replies cannot stall us. The peer already passed the handshake, so any failure here ends
/// the phase and keeps what was gathered; an unanswered check stays unverified.
async fn after_handshake(
    stream: &mut TcpStream,
    preset: &CoinPreset,
    opts: &ProbeOptions,
    checkpoint: Option<&Checkpoint>,
    out: &mut AfterHandshake,
) {
    let start = Instant::now();
    let mut headers_due = checkpoint.map(|_| start + opts.handshake_timeout);
    let mut addrs_due = opts.want_addrs.then(|| start + opts.addr_wait);
    let Some(last) = headers_due.max(addrs_due) else {
        return;
    };
    if checkpoint.is_some() {
        out.chain = ChainCheck::Unverified;
    }
    let mut shortened = false;
    let _ = timeout_at(last, async {
        if let Some(c) = checkpoint {
            let locator = encode_getheaders(preset.protocol_version, &[c.hash]);
            send(stream, preset, "getheaders", &locator).await?;
        }
        if addrs_due.is_some() {
            send(stream, preset, "getaddr", &[]).await?;
        }
        // A read is cancelled only at the last deadline: cancelling one mid-frame would
        // desynchronise the stream. A reply that arrives after its own deadline is ignored.
        while let Some(until) = headers_due.max(addrs_due) {
            let read = read_frame_tolerant(stream, preset.wire());
            let Ok(frame) = timeout_at(until, read).await else {
                break;
            };
            let frame = frame?;
            let now = Instant::now();
            headers_due = headers_due.filter(|d| *d > now);
            addrs_due = addrs_due.filter(|d| *d > now);
            match frame.command.as_str() {
                // We never send `sendheaders`, so peers announce new blocks with `inv`; the
                // only `headers` a peer sends us is the reply to our `getheaders`.
                "headers" if headers_due.is_some() => {
                    if let Some(c) = checkpoint {
                        out.chain = chain_verdict(&frame.payload, c);
                    }
                    headers_due = None;
                }
                "addr" | "addrv2" if addrs_due.is_some() => {
                    let entries = if frame.command == "addr" {
                        decode_addr(&frame.payload)
                    } else {
                        decode_addrv2(&frame.payload)
                    };
                    let Ok(entries) = entries else { continue };
                    if entries.len() > 1 && !shortened {
                        shortened = true;
                        addrs_due = addrs_due.map(|d| d.min(Instant::now() + GRACE));
                    }
                    out.addrs.extend(entries);
                    if out.addrs.len() >= MAX_HARVEST {
                        addrs_due = None;
                    }
                }
                "ping" => reply_pong(stream, preset, &frame.payload).await?,
                _ => {}
            }
        }
        Ok::<(), ProbeError>(())
    })
    .await;
    out.addrs.truncate(MAX_HARVEST);
}
