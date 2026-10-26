//! Message framing: 24-byte header (magic, command, length, checksum) plus payload.

use groestl::Groestl512;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::coin::{Checksum, CoinPreset};

/// Largest payload we accept; a larger length header is rejected before any payload is read.
pub const MAX_PAYLOAD: u32 = 1 << 20;

/// Largest payload `read_frame_tolerant` will stream past; larger is fatal.
pub const MAX_SKIP: u32 = 32 << 20;

/// Bytes of an oversize payload that `read_frame_tolerant` keeps.
pub const PREFIX_KEEP: usize = 64;

const SKIP_CHUNK: usize = 64 << 10;
const HEADER_LEN: usize = 24;
const COMMAND_LEN: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub command: String,
    pub payload: Vec<u8>,
    /// True when the payload was over `MAX_PAYLOAD` (1 MiB): only its first `PREFIX_KEEP`
    /// bytes are kept. Shorter payloads are always whole.
    pub truncated: bool,
}

/// Framing parameters of a chain: network magic and header checksum variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Wire {
    pub magic: [u8; 4],
    pub checksum: Checksum,
}

impl CoinPreset {
    pub fn wire(&self) -> Wire {
        Wire {
            magic: self.magic,
            checksum: self.checksum,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("wrong network magic")]
    BadMagic,
    #[error("payload length {0} exceeds the limit")]
    TooLarge(u32),
    #[error("bad payload checksum")]
    BadChecksum,
    #[error("message truncated")]
    Truncated,
    #[error("malformed message: {0}")]
    Malformed(&'static str),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

/// Streaming checksum state for one payload.
enum Hasher {
    Sha256d(Sha256),
    Groestl512d(Groestl512),
}

impl Hasher {
    fn new(kind: Checksum) -> Self {
        match kind {
            Checksum::Sha256d => Hasher::Sha256d(Sha256::new()),
            Checksum::Groestl512d => Hasher::Groestl512d(Groestl512::new()),
        }
    }

    fn update(&mut self, data: &[u8]) {
        match self {
            Hasher::Sha256d(h) => h.update(data),
            Hasher::Groestl512d(h) => h.update(data),
        }
    }

    /// Hashes the first digest again (whole, 64 bytes for Groestl) and keeps 4 bytes.
    fn finalize(self) -> [u8; 4] {
        match self {
            Hasher::Sha256d(h) => Sha256::digest(h.finalize())[..4].try_into().unwrap(),
            Hasher::Groestl512d(h) => Groestl512::digest(h.finalize())[..4].try_into().unwrap(),
        }
    }
}

fn checksum(kind: Checksum, payload: &[u8]) -> [u8; 4] {
    let mut h = Hasher::new(kind);
    h.update(payload);
    h.finalize()
}

/// Builds a complete frame. `command` longer than 12 bytes is truncated.
pub fn encode_frame(wire: Wire, command: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&wire.magic);
    let mut cmd = [0u8; COMMAND_LEN];
    let n = command.len().min(COMMAND_LEN);
    cmd[..n].copy_from_slice(&command.as_bytes()[..n]);
    out.extend_from_slice(&cmd);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&checksum(wire.checksum, payload));
    out.extend_from_slice(payload);
    out
}

async fn read_exact<R: AsyncRead + Unpin>(r: &mut R, buf: &mut [u8]) -> Result<(), WireError> {
    match r.read_exact(buf).await {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Err(WireError::Truncated),
        Err(e) => Err(e.into()),
    }
}

fn parse_command(raw: &[u8]) -> Result<String, WireError> {
    let end = raw.iter().position(|b| *b == 0).unwrap_or(raw.len());
    let (name, pad) = raw.split_at(end);
    if !name.iter().all(|b| (0x20..0x7f).contains(b)) {
        return Err(WireError::Malformed("command is not printable ASCII"));
    }
    if pad.iter().any(|b| *b != 0) {
        return Err(WireError::Malformed("data after command padding"));
    }
    Ok(String::from_utf8_lossy(name).into_owned())
}

/// Parses and validates a header; returns the command and declared length.
fn parse_header(header: &[u8; HEADER_LEN], wire: Wire) -> Result<(String, u32), WireError> {
    if header[..4] != wire.magic {
        return Err(WireError::BadMagic);
    }
    let len = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
    Ok((parse_command(&header[4..16])?, len))
}

/// Reads one frame. Magic and length are validated before the payload is read.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R, wire: Wire) -> Result<Frame, WireError> {
    let mut header = [0u8; HEADER_LEN];
    read_exact(r, &mut header).await?;
    let (command, len) = parse_header(&header, wire)?;
    if len > MAX_PAYLOAD {
        return Err(WireError::TooLarge(len));
    }
    let mut payload = vec![0u8; len as usize];
    read_exact(r, &mut payload).await?;
    if header[20..24] != checksum(wire.checksum, &payload) {
        return Err(WireError::BadChecksum);
    }
    Ok(Frame {
        command,
        payload,
        truncated: false,
    })
}

/// Like `read_frame`, but a payload above `MAX_PAYLOAD` (up to `MAX_SKIP`) is streamed through
/// the checksum in fixed chunks, keeping only its first `PREFIX_KEEP` bytes. The stream stays
/// in sync and the checksum is still verified.
pub async fn read_frame_tolerant<R: AsyncRead + Unpin>(
    r: &mut R,
    wire: Wire,
) -> Result<Frame, WireError> {
    let mut header = [0u8; HEADER_LEN];
    read_exact(r, &mut header).await?;
    let (command, len) = parse_header(&header, wire)?;
    if len > MAX_SKIP {
        return Err(WireError::TooLarge(len));
    }
    if len <= MAX_PAYLOAD {
        let mut payload = vec![0u8; len as usize];
        read_exact(r, &mut payload).await?;
        if header[20..24] != checksum(wire.checksum, &payload) {
            return Err(WireError::BadChecksum);
        }
        return Ok(Frame {
            command,
            payload,
            truncated: false,
        });
    }
    let mut hasher = Hasher::new(wire.checksum);
    let mut prefix = Vec::with_capacity(PREFIX_KEEP);
    let mut chunk = vec![0u8; SKIP_CHUNK];
    let mut left = len as usize;
    while left > 0 {
        let n = left.min(SKIP_CHUNK);
        read_exact(r, &mut chunk[..n]).await?;
        hasher.update(&chunk[..n]);
        let room = PREFIX_KEEP - prefix.len();
        prefix.extend_from_slice(&chunk[..n.min(room)]);
        left -= n;
    }
    if header[20..24] != hasher.finalize() {
        return Err(WireError::BadChecksum);
    }
    Ok(Frame {
        command,
        payload: prefix,
        truncated: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAGIC: [u8; 4] = [0xf9, 0xbe, 0xb4, 0xd9];
    const SHA: Wire = Wire {
        magic: MAGIC,
        checksum: Checksum::Sha256d,
    };

    async fn read(bytes: &[u8], magic: [u8; 4]) -> Result<Frame, WireError> {
        let mut r = bytes;
        let wire = Wire {
            magic,
            checksum: Checksum::Sha256d,
        };
        read_frame(&mut r, wire).await
    }

    #[tokio::test]
    async fn frame_roundtrip() {
        let bytes = encode_frame(SHA, "verack", &[]);
        assert_eq!(bytes.len(), 24);
        assert_eq!(&bytes[20..24], &[0x5d, 0xf6, 0xe0, 0xe2]);
        let f = read(&bytes, MAGIC).await.unwrap();
        assert_eq!(f.command, "verack");
        assert!(f.payload.is_empty());

        let bytes = encode_frame(SHA, "ping", &[1, 2, 3]);
        let f = read(&bytes, MAGIC).await.unwrap();
        assert_eq!((f.command.as_str(), f.payload), ("ping", vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn frame_rejects_wrong_magic() {
        let bytes = encode_frame(SHA, "verack", &[]);
        assert!(matches!(
            read(&bytes, [1, 2, 3, 4]).await,
            Err(WireError::BadMagic)
        ));
    }

    #[tokio::test]
    async fn frame_rejects_bad_checksum() {
        let mut bytes = encode_frame(SHA, "ping", &[9; 8]);
        bytes[24] ^= 0xff;
        assert!(matches!(
            read(&bytes, MAGIC).await,
            Err(WireError::BadChecksum)
        ));
    }

    #[tokio::test]
    async fn frame_rejects_truncated() {
        let bytes = encode_frame(SHA, "ping", &[9; 8]);
        assert!(matches!(
            read(&bytes[..30], MAGIC).await,
            Err(WireError::Truncated)
        ));
        assert!(matches!(
            read(&bytes[..10], MAGIC).await,
            Err(WireError::Truncated)
        ));
        assert!(matches!(read(&[], MAGIC).await, Err(WireError::Truncated)));
    }

    #[tokio::test]
    async fn frame_rejects_oversized_length_without_reading_it() {
        let mut bytes = encode_frame(SHA, "block", &[]);
        bytes[16..20].copy_from_slice(&30_000_000u32.to_le_bytes());
        assert!(matches!(
            read(&bytes, MAGIC).await,
            Err(WireError::TooLarge(30_000_000))
        ));
    }

    #[tokio::test]
    async fn frame_rejects_garbage_command() {
        let mut bytes = encode_frame(SHA, "ping", &[]);
        bytes[4] = 0xff;
        assert!(matches!(
            read(&bytes, MAGIC).await,
            Err(WireError::Malformed(_))
        ));
        let mut bytes = encode_frame(SHA, "ping", &[]);
        bytes[4 + 8] = b'x'; // data after the NUL padding starts
        assert!(matches!(
            read(&bytes, MAGIC).await,
            Err(WireError::Malformed(_))
        ));
    }

    #[test]
    fn groestl_checksum_matches_reference_vectors() {
        // Vectors from Groestlcoin's reference binding, which reproduces Groestlcoin's genesis hash.
        let g = Wire {
            magic: MAGIC,
            checksum: Checksum::Groestl512d,
        };
        assert_eq!(
            &encode_frame(g, "verack", &[])[20..24],
            &[0xfd, 0xfb, 0x14, 0xd3]
        );
        assert_eq!(
            &encode_frame(g, "ping", &[1, 2, 3])[20..24],
            &[0x04, 0x97, 0xca, 0x2e]
        );
    }

    #[tokio::test]
    async fn checksum_variant_must_match() {
        let g = Wire {
            magic: MAGIC,
            checksum: Checksum::Groestl512d,
        };
        let s = Wire {
            magic: MAGIC,
            checksum: Checksum::Sha256d,
        };
        let bytes = encode_frame(g, "ping", &[9; 8]);
        assert!(read_frame(&mut &bytes[..], g).await.is_ok());
        assert!(matches!(
            read_frame(&mut &bytes[..], s).await,
            Err(WireError::BadChecksum)
        ));
    }

    #[tokio::test]
    async fn tolerant_read_rejects_bad_checksum_on_small_frame() {
        let ok = encode_frame(SHA, "ping", &[9; 8]);
        let f = read_frame_tolerant(&mut &ok[..], SHA).await.unwrap();
        assert_eq!((f.command.as_str(), f.payload), ("ping", vec![9; 8]));
        let mut bad = ok;
        bad[27] ^= 0x01; // one payload byte, header untouched
        assert!(matches!(
            read_frame_tolerant(&mut &bad[..], SHA).await,
            Err(WireError::BadChecksum)
        ));
    }

    #[tokio::test]
    async fn tolerant_read_skips_oversize_but_verifies_it() {
        let s = Wire {
            magic: MAGIC,
            checksum: Checksum::Sha256d,
        };
        let big: Vec<u8> = (0..3_000_000u32).map(|i| i as u8).collect();
        let mut bytes = encode_frame(s, "inv", &big);
        bytes.extend(encode_frame(s, "ping", &[1; 8]));
        let mut r = &bytes[..];
        let f = read_frame_tolerant(&mut r, s).await.unwrap();
        assert!(f.truncated && f.payload == big[..64]);
        assert_eq!(
            read_frame_tolerant(&mut r, s).await.unwrap().command,
            "ping"
        );
        let mut bad = encode_frame(s, "inv", &big);
        bad[30] ^= 1;
        assert!(matches!(
            read_frame_tolerant(&mut &bad[..], s).await,
            Err(WireError::BadChecksum)
        ));
        let mut huge = encode_frame(s, "block", &[]);
        huge[16..20].copy_from_slice(&(33u32 << 20).to_le_bytes());
        assert!(matches!(
            read_frame_tolerant(&mut &huge[..], s).await,
            Err(WireError::TooLarge(_))
        ));
    }
}
