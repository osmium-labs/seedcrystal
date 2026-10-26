//! Bitcoin P2P wire codec: message framing and the handful of payloads a seeder needs.

pub mod frame;
pub mod messages;
pub mod probe;

pub use frame::{
    Frame, MAX_PAYLOAD, MAX_SKIP, PREFIX_KEEP, Wire, WireError, encode_frame, read_frame,
    read_frame_tolerant,
};
pub use messages::{
    AddrEntry, NetAddr, VersionMsg, decode_addr, decode_addrv2, decode_headers_first_prev,
    decode_ping, decode_version, encode_addr, encode_addrv2, encode_getheaders, encode_ping,
    encode_version,
};
pub use probe::{ChainCheck, ProbeError, ProbeOptions, ProbeResult, probe};
