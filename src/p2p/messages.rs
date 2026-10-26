//! Payload codecs: `version`, `ping`/`pong`, `addr` and BIP155 `addrv2`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use super::frame::WireError;
use crate::coin::VersionTrailer;

/// Most entries accepted in one `addr` or `addrv2` message.
const MAX_ADDR_ENTRIES: u64 = 1000;
/// Longest address field accepted in an `addrv2` entry (BIP155).
const MAX_ADDRV2_LEN: u64 = 512;
/// Longest user agent accepted in `version`.
const MAX_USER_AGENT: u64 = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionMsg {
    pub version: i32,
    pub services: u64,
    pub timestamp: i64,
    pub receiver: SocketAddr,
    pub nonce: u64,
    pub user_agent: String,
    pub start_height: i32,
    pub relay: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NetAddr {
    V4(Ipv4Addr),
    V6(Ipv6Addr),
    TorV3([u8; 32]),
    I2p([u8; 32]),
    Cjdns([u8; 16]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddrEntry {
    pub addr: NetAddr,
    pub port: u16,
    pub services: u64,
    pub time: u32,
}

struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        if self.buf.len() < n {
            return Err(WireError::Truncated);
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], WireError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }

    fn u16_be(&mut self) -> Result<u16, WireError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn compact(&mut self) -> Result<u64, WireError> {
        Ok(match self.u8()? {
            0xfd => u16::from_le_bytes(self.array()?).into(),
            0xfe => self.u32()?.into(),
            0xff => self.u64()?,
            n => n.into(),
        })
    }

    /// An IPv6-style 16-byte field plus big-endian port, as used inside `version`.
    fn socket_addr(&mut self) -> Result<SocketAddr, WireError> {
        let ip = ip_from_16(self.array()?);
        Ok(SocketAddr::new(ip, self.u16_be()?))
    }
}

fn ip_from_16(raw: [u8; 16]) -> IpAddr {
    let v6 = Ipv6Addr::from(raw);
    match v6.to_ipv4_mapped() {
        Some(v4) => IpAddr::V4(v4),
        None => IpAddr::V6(v6),
    }
}

fn put_compact(out: &mut Vec<u8>, n: u64) {
    match n {
        0..=0xfc => out.push(n as u8),
        0xfd..=0xffff => {
            out.push(0xfd);
            out.extend((n as u16).to_le_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(0xfe);
            out.extend((n as u32).to_le_bytes());
        }
        _ => {
            out.push(0xff);
            out.extend(n.to_le_bytes());
        }
    }
}

fn ip_to_16(ip: IpAddr) -> [u8; 16] {
    match ip {
        IpAddr::V4(v4) => v4.to_ipv6_mapped().octets(),
        IpAddr::V6(v6) => v6.octets(),
    }
}

/// Encodes a `version` payload. Services are advertised as given; `addr_from` is zeroed.
/// `VersionTrailer::Dash` appends a random 32-byte challenge and a false masternode flag.
pub fn encode_version(v: &VersionMsg, trailer: VersionTrailer) -> Vec<u8> {
    let mut out = Vec::with_capacity(120 + v.user_agent.len());
    out.extend(v.version.to_le_bytes());
    out.extend(v.services.to_le_bytes());
    out.extend(v.timestamp.to_le_bytes());
    out.extend(0u64.to_le_bytes());
    out.extend(ip_to_16(v.receiver.ip()));
    out.extend(v.receiver.port().to_be_bytes());
    out.extend([0u8; 26]);
    out.extend(v.nonce.to_le_bytes());
    put_compact(&mut out, v.user_agent.len() as u64);
    out.extend(v.user_agent.as_bytes());
    out.extend(v.start_height.to_le_bytes());
    out.push(v.relay as u8);
    if trailer == VersionTrailer::Dash {
        out.extend(rand::random::<[u8; 32]>());
        out.push(0);
    }
    out
}

/// Decodes the leading fields of a `version` payload; trailing fields are ignored and a
/// missing `relay` byte means relay is on.
pub fn decode_version(payload: &[u8]) -> Result<VersionMsg, WireError> {
    let mut r = Reader { buf: payload };
    let version = r.u32()? as i32;
    let services = r.u64()?;
    let timestamp = r.u64()? as i64;
    r.take(8)?; // receiver services
    let receiver = r.socket_addr()?;
    r.take(26)?; // addr_from
    let nonce = r.u64()?;
    let ua_len = r.compact()?;
    if ua_len > MAX_USER_AGENT {
        return Err(WireError::Malformed("user agent too long"));
    }
    let user_agent = String::from_utf8_lossy(r.take(ua_len as usize)?).into_owned();
    let start_height = r.u32()? as i32;
    let relay = r.u8().map(|b| b != 0).unwrap_or(true);
    Ok(VersionMsg {
        version,
        services,
        timestamp,
        receiver,
        nonce,
        user_agent,
        start_height,
        relay,
    })
}

/// Payload of both `ping` and `pong`.
pub fn encode_ping(nonce: u64) -> Vec<u8> {
    nonce.to_le_bytes().to_vec()
}

pub fn decode_ping(payload: &[u8]) -> Result<u64, WireError> {
    Reader { buf: payload }.u64()
}

/// Encodes a `getheaders` payload: version, locator hashes (wire order) and a zero stop hash.
pub fn encode_getheaders(version: i32, locator: &[[u8; 32]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 9 + locator.len() * 32 + 32);
    out.extend((version as u32).to_le_bytes());
    put_compact(&mut out, locator.len() as u64);
    for h in locator {
        out.extend_from_slice(h);
    }
    out.extend_from_slice(&[0u8; 32]);
    out
}

/// Reads `hashPrevBlock` of the first header in a `headers` payload; `None` when it is empty.
/// Needs only the first 36 bytes after the count (version and previous-block hash), so a
/// 64-byte prefix suffices.
pub fn decode_headers_first_prev(payload: &[u8]) -> Result<Option<[u8; 32]>, WireError> {
    let mut r = Reader { buf: payload };
    if r.compact()? == 0 {
        return Ok(None);
    }
    r.take(4)?;
    Ok(Some(r.array()?))
}

fn read_count(r: &mut Reader) -> Result<usize, WireError> {
    let n = r.compact()?;
    if n > MAX_ADDR_ENTRIES {
        return Err(WireError::Malformed("too many addresses"));
    }
    Ok(n as usize)
}

/// Decodes a v1 `addr` payload (entries carry a timestamp).
pub fn decode_addr(payload: &[u8]) -> Result<Vec<AddrEntry>, WireError> {
    let mut r = Reader { buf: payload };
    let count = read_count(&mut r)?;
    let mut out = Vec::with_capacity(count.min(r.buf.len() / 30));
    for _ in 0..count {
        let time = r.u32()?;
        let services = r.u64()?;
        let raw: [u8; 16] = r.array()?;
        let port = r.u16_be()?;
        let addr = match ip_from_16(raw) {
            IpAddr::V4(v4) => NetAddr::V4(v4),
            IpAddr::V6(v6) => NetAddr::V6(v6),
        };
        out.push(AddrEntry {
            addr,
            port,
            services,
            time,
        });
    }
    Ok(out)
}

/// Encodes a v1 `addr` payload. Overlay-network entries have no v1 form and are dropped.
pub fn encode_addr(entries: &[AddrEntry]) -> Vec<u8> {
    let rows: Vec<(&AddrEntry, [u8; 16])> = entries
        .iter()
        .filter_map(|e| match e.addr {
            NetAddr::V4(a) => Some((e, a.to_ipv6_mapped().octets())),
            NetAddr::V6(a) => Some((e, a.octets())),
            _ => None,
        })
        .collect();
    let mut out = Vec::new();
    put_compact(&mut out, rows.len() as u64);
    for (e, ip) in rows {
        out.extend(e.time.to_le_bytes());
        out.extend(e.services.to_le_bytes());
        out.extend(ip);
        out.extend(e.port.to_be_bytes());
    }
    out
}

/// Decodes a BIP155 `addrv2` payload. Unknown network ids (and deprecated TorV2) are skipped;
/// a known id with the wrong address length is malformed.
pub fn decode_addrv2(payload: &[u8]) -> Result<Vec<AddrEntry>, WireError> {
    let mut r = Reader { buf: payload };
    let count = read_count(&mut r)?;
    let mut out = Vec::with_capacity(count.min(r.buf.len() / 9));
    for _ in 0..count {
        let time = r.u32()?;
        let services = r.compact()?;
        let network = r.u8()?;
        let len = r.compact()?;
        if len > MAX_ADDRV2_LEN {
            return Err(WireError::Malformed("addrv2 address too long"));
        }
        let raw = r.take(len as usize)?;
        let port = r.u16_be()?;
        let bad_len = WireError::Malformed("addrv2 address length does not match network");
        let addr = match network {
            1 => NetAddr::V4(Ipv4Addr::from(
                <[u8; 4]>::try_from(raw).map_err(|_| bad_len)?,
            )),
            2 => NetAddr::V6(Ipv6Addr::from(
                <[u8; 16]>::try_from(raw).map_err(|_| bad_len)?,
            )),
            4 => NetAddr::TorV3(raw.try_into().map_err(|_| bad_len)?),
            5 => NetAddr::I2p(raw.try_into().map_err(|_| bad_len)?),
            6 => NetAddr::Cjdns(raw.try_into().map_err(|_| bad_len)?),
            _ => continue,
        };
        out.push(AddrEntry {
            addr,
            port,
            services,
            time,
        });
    }
    Ok(out)
}

/// Encodes a BIP155 `addrv2` payload.
pub fn encode_addrv2(entries: &[AddrEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    put_compact(&mut out, entries.len() as u64);
    for e in entries {
        out.extend(e.time.to_le_bytes());
        put_compact(&mut out, e.services);
        let (network, bytes): (u8, Vec<u8>) = match e.addr {
            NetAddr::V4(a) => (1, a.octets().to_vec()),
            NetAddr::V6(a) => (2, a.octets().to_vec()),
            NetAddr::TorV3(a) => (4, a.to_vec()),
            NetAddr::I2p(a) => (5, a.to_vec()),
            NetAddr::Cjdns(a) => (6, a.to_vec()),
        };
        out.push(network);
        put_compact(&mut out, bytes.len() as u64);
        out.extend(bytes);
        out.extend(e.port.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coin::VersionTrailer;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

    fn sample() -> VersionMsg {
        VersionMsg {
            version: 70016,
            services: 0,
            timestamp: 1_700_000_000,
            receiver: "203.0.114.7:9969".parse().unwrap(),
            nonce: 0x0102_0304_0506_0708,
            user_agent: "/seedcrystal:0.1/".into(),
            start_height: 12345,
            relay: false,
        }
    }

    #[test]
    fn version_roundtrip_bitcoin() {
        let v = sample();
        let p = encode_version(&v, VersionTrailer::None);
        assert_eq!(p.len(), 85 + 1 + v.user_agent.len());
        assert_eq!(decode_version(&p).unwrap(), v);
        // addr_from is all zeros
        assert!(p[46..72].iter().all(|b| *b == 0));
        // receiver: services 0, mapped ip, big-endian port
        assert_eq!(&p[20..28], &[0u8; 8]);
        assert_eq!(&p[28..38], &[0u8; 10]);
        assert_eq!(&p[38..44], &[0xff, 0xff, 203, 0, 114, 7]);
        assert_eq!(&p[44..46], &9969u16.to_be_bytes());
    }

    /// A `version` payload with fixed fields, derived by hand field by field.
    fn version_vector() -> (VersionMsg, Vec<u8>) {
        let msg = VersionMsg {
            version: 70016,
            services: 0x409,
            timestamp: 1_700_000_000,
            receiver: "192.0.2.1:8333".parse().unwrap(),
            nonce: 0x0102_0304_0506_0708,
            user_agent: "/t:1/".into(),
            start_height: 500_000,
            relay: false,
        };
        let mut bytes = Vec::new();
        bytes.extend([0x80, 0x11, 0x01, 0x00]); // version 70016 = 0x00011180, LE
        bytes.extend([0x09, 0x04, 0, 0, 0, 0, 0, 0]); // services 0x409, LE
        bytes.extend([0x00, 0xf1, 0x53, 0x65, 0, 0, 0, 0]); // timestamp 1700000000 = 0x6553f100, LE
        bytes.extend([0; 8]); // receiver services
        bytes.extend([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 0, 2, 1]); // receiver ip, v4-mapped
        bytes.extend([0x20, 0x8d]); // receiver port 8333 = 0x208d, BE
        bytes.extend([0; 26]); // addr_from: services, ip, port, all zero
        bytes.extend([0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]); // nonce, LE
        bytes.extend([0x05, b'/', b't', b':', b'1', b'/']); // user agent: length 5, then text
        bytes.extend([0x20, 0xa1, 0x07, 0x00]); // start height 500000 = 0x0007a120, LE
        bytes.push(0x00); // relay false
        (msg, bytes)
    }

    #[test]
    fn encode_version_matches_hand_built_vector() {
        let (msg, bytes) = version_vector();
        assert_eq!(bytes.len(), 91);
        assert_eq!(encode_version(&msg, VersionTrailer::None), bytes);
    }

    #[test]
    fn decode_version_reads_every_field_of_hand_built_vector() {
        let (msg, bytes) = version_vector();
        assert_eq!(decode_version(&bytes).unwrap(), msg);
    }

    #[test]
    fn version_roundtrip_v6_receiver() {
        let mut v = sample();
        v.receiver = SocketAddr::new(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1).into(), 1);
        assert_eq!(
            decode_version(&encode_version(&v, VersionTrailer::None)).unwrap(),
            v
        );
    }

    #[test]
    fn dash_version_appends_challenge_and_false_masternode_flag() {
        let v = sample();
        let btc = encode_version(&v, VersionTrailer::None);
        let dash = encode_version(&v, VersionTrailer::Dash);
        assert_eq!(dash.len(), btc.len() + 33);
        assert_eq!(&dash[..btc.len()], &btc[..]);
        assert_eq!(*dash.last().unwrap(), 0x00);
        assert_eq!(decode_version(&dash).unwrap(), v);
    }

    #[test]
    fn decode_version_ignores_trailing_fields() {
        let v = sample();
        let mut p = encode_version(&v, VersionTrailer::None);
        p.extend((0..33u8).map(|b| b.wrapping_mul(7).wrapping_add(1)));
        assert_eq!(decode_version(&p).unwrap(), v);
    }

    #[test]
    fn decode_version_handles_old_and_hostile_payloads() {
        let v = sample();
        let p = encode_version(&v, VersionTrailer::None);
        // no relay byte: defaults to true
        let d = decode_version(&p[..p.len() - 1]).unwrap();
        assert!(d.relay);
        // every truncation shorter than that is an error, never a panic
        for n in 0..p.len() - 1 {
            assert!(decode_version(&p[..n]).is_err(), "len {n}");
        }
        // absurd user agent length
        let mut bad = p[..80].to_vec();
        bad.extend([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert!(decode_version(&bad).is_err());
    }

    #[test]
    fn ping_roundtrip() {
        let p = encode_ping(0xdead_beef_0102_0304);
        assert_eq!(p, 0xdead_beef_0102_0304u64.to_le_bytes());
        assert_eq!(decode_ping(&p).unwrap(), 0xdead_beef_0102_0304);
        assert!(decode_ping(&p[..7]).is_err());
        assert_eq!(
            decode_ping(&[p.as_slice(), &[1]].concat()).unwrap(),
            0xdead_beef_0102_0304
        );
    }

    #[test]
    fn addr_v1_decodes_ipv4_mapped() {
        let mut p = vec![1u8]; // count
        p.extend(1_700_000_000u32.to_le_bytes());
        p.extend(1u64.to_le_bytes());
        p.extend([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 203, 0, 114, 7]);
        p.extend(9969u16.to_be_bytes());
        let e = decode_addr(&p).unwrap();
        assert_eq!(
            e,
            vec![AddrEntry {
                addr: NetAddr::V4(Ipv4Addr::new(203, 0, 114, 7)),
                port: 9969,
                services: 1,
                time: 1_700_000_000
            }]
        );
        assert_eq!(encode_addr(&e), p);
    }

    #[test]
    fn addr_v1_roundtrip_and_truncation() {
        let entries = vec![
            AddrEntry {
                addr: NetAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
                port: 1,
                services: 5,
                time: 6,
            },
            AddrEntry {
                addr: NetAddr::V6("2001:db8::1".parse().unwrap()),
                port: 2,
                services: 7,
                time: 8,
            },
        ];
        let p = encode_addr(&entries);
        assert_eq!(decode_addr(&p).unwrap(), entries);
        for n in 0..p.len() {
            assert!(decode_addr(&p[..n]).is_err(), "len {n}");
        }
    }

    fn v2_entry(time: u32, services: u8, net: u8, addr: &[u8], port: u16) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend(time.to_le_bytes());
        p.push(services); // compact size < 253
        p.push(net);
        p.push(addr.len() as u8);
        p.extend(addr);
        p.extend(port.to_be_bytes());
        p
    }

    #[test]
    fn addrv2_decodes_v4_v6_torv3_and_skips_unknown_network() {
        let v6: Ipv6Addr = "2001:db8::2".parse().unwrap();
        let mut p = vec![4u8];
        p.extend(v2_entry(10, 1, 1, &[203, 0, 114, 7], 9969));
        p.extend(v2_entry(11, 5, 2, &v6.octets(), 9970));
        p.extend(v2_entry(12, 0, 0x7f, &[9; 40], 1));
        p.extend(v2_entry(13, 1, 4, &[0xab; 32], 9971));
        let e = decode_addrv2(&p).unwrap();
        assert_eq!(e.len(), 3);
        assert_eq!(e[0].addr, NetAddr::V4(Ipv4Addr::new(203, 0, 114, 7)));
        assert_eq!((e[0].port, e[0].services, e[0].time), (9969, 1, 10));
        assert_eq!(e[1].addr, NetAddr::V6(v6));
        assert_eq!(e[1].services, 5);
        assert_eq!(e[2].addr, NetAddr::TorV3([0xab; 32]));
        assert_eq!(e[2].port, 9971);
    }

    #[test]
    fn addrv2_i2p_cjdns_torv2_and_bad_lengths() {
        let mut p = vec![3u8];
        p.extend(v2_entry(1, 0, 5, &[1; 32], 1));
        p.extend(v2_entry(2, 0, 6, &[2; 16], 2));
        p.extend(v2_entry(3, 0, 3, &[3; 10], 3)); // TorV2: skipped
        let e = decode_addrv2(&p).unwrap();
        assert_eq!(e.len(), 2);
        assert_eq!(e[0].addr, NetAddr::I2p([1; 32]));
        assert_eq!(e[1].addr, NetAddr::Cjdns([2; 16]));

        for (net, len) in [(1u8, 5usize), (2, 4), (4, 31), (5, 16), (6, 32)] {
            let mut p = vec![1u8];
            p.extend(v2_entry(1, 0, net, &vec![0; len], 1));
            assert!(
                matches!(decode_addrv2(&p), Err(WireError::Malformed(_))),
                "net {net}"
            );
        }
    }

    #[test]
    fn addrv2_roundtrip_and_hostile() {
        let entries = vec![
            AddrEntry {
                addr: NetAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
                port: 1,
                services: 0x1_0000_0000,
                time: 6,
            },
            AddrEntry {
                addr: NetAddr::V6("2001:db8::1".parse().unwrap()),
                port: 2,
                services: 7,
                time: 8,
            },
            AddrEntry {
                addr: NetAddr::TorV3([7; 32]),
                port: 3,
                services: 300,
                time: 9,
            },
            AddrEntry {
                addr: NetAddr::I2p([8; 32]),
                port: 4,
                services: 0,
                time: 10,
            },
            AddrEntry {
                addr: NetAddr::Cjdns([9; 16]),
                port: 5,
                services: 1,
                time: 11,
            },
        ];
        let p = encode_addrv2(&entries);
        assert_eq!(decode_addrv2(&p).unwrap(), entries);
        for n in 0..p.len() {
            assert!(decode_addrv2(&p[..n]).is_err(), "len {n}");
        }
        // address length over the 512 bound
        let mut big = vec![1u8];
        big.extend(1u32.to_le_bytes());
        big.extend([0, 1, 0xfd]);
        big.extend(513u16.to_le_bytes());
        big.extend(vec![0; 600]);
        assert!(matches!(decode_addrv2(&big), Err(WireError::Malformed(_))));
    }

    #[test]
    fn encode_addrv2_matches_hand_built_vector() {
        let entries = [
            AddrEntry {
                addr: NetAddr::V4(Ipv4Addr::new(203, 0, 114, 7)),
                port: 9969,
                services: 300,
                time: 0x0102_0304,
            },
            AddrEntry {
                addr: NetAddr::TorV3([0xab; 32]),
                port: 1,
                services: 1,
                time: 5,
            },
        ];
        let mut expected = vec![0x02];
        expected.extend([0x04, 0x03, 0x02, 0x01]); // time, LE
        expected.extend([0xfd, 0x2c, 0x01]); // services 300 as compact size
        expected.extend([0x01, 0x04, 203, 0, 114, 7]); // IPv4, len 4
        expected.extend([0x26, 0xf1]); // port 9969, BE
        expected.extend([0x05, 0, 0, 0]);
        expected.extend([0x01, 0x04, 0x20]); // services 1, TorV3, len 32
        expected.extend([0xab; 32]);
        expected.extend([0x00, 0x01]);
        assert_eq!(encode_addrv2(&entries), expected);
    }

    #[test]
    fn addr_count_over_1000_is_malformed() {
        for decode in [decode_addr, decode_addrv2] {
            let mut p = vec![0xfd];
            p.extend(1001u16.to_le_bytes());
            assert!(matches!(decode(&p), Err(WireError::Malformed(_))));
            // 1000 is allowed by the cap (then runs out of data)
            let mut p = vec![0xfd];
            p.extend(1000u16.to_le_bytes());
            assert!(matches!(decode(&p), Err(WireError::Truncated)));
            // a count claiming 2^32 entries must not allocate
            let p = [0xfe, 0xff, 0xff, 0xff, 0xff];
            assert!(decode(&p).is_err());
        }
    }

    #[test]
    fn getheaders_layout_and_headers_prefix() {
        let loc = [[0xab; 32]];
        let p = encode_getheaders(70220, &loc);
        assert_eq!(p.len(), 4 + 1 + 32 + 32);
        assert_eq!(&p[..4], &70220u32.to_le_bytes());
        assert_eq!(
            (p[4], &p[5..37], &p[37..]),
            (1, &[0xab; 32][..], &[0u8; 32][..])
        );
        assert_eq!(decode_headers_first_prev(&[0]).unwrap(), None);
        let mut h = vec![2u8]; // count
        h.extend(1i32.to_le_bytes()); // header version
        h.extend([0xcd; 32]); // hashPrevBlock
        h.extend([0u8; 20]); // rest of the 64-byte prefix
        assert_eq!(decode_headers_first_prev(&h).unwrap(), Some([0xcd; 32]));
        assert!(decode_headers_first_prev(&h[..20]).is_err());
    }
}
