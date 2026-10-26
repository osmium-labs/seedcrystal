use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use hickory_proto::op::{Edns, Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA, NS, SOA};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use rand::Rng;
use rand::seq::SliceRandom;

use crate::crawler::{Snapshot, SnapshotRx};
use crate::store::Servable;

const TTL_ADDR: u32 = 60;
const TTL_SOA: u32 = 900;
const TTL_NS: u32 = 86_400;
const SOA_REFRESH: i32 = 3600;
const SOA_RETRY: i32 = 600;
const SOA_EXPIRE: i32 = 604_800;
const SOA_MINIMUM: u32 = 60;
const MAX_A: usize = 20;
const MAX_AAAA_PLAIN: usize = 15;
const MAX_AAAA_EDNS: usize = 20;
/// Payload size advertised in our OPT record.
pub const ADVERTISED_PAYLOAD: u16 = 1232;

/// One served domain.
pub struct Zone {
    pub apex: Name,
    pub nameserver: Name,
    pub mailbox: Name,
    pub seed_flags: u64,
    pub snapshot: SnapshotRx,
}

pub struct ZoneSet(Vec<Zone>);

impl ZoneSet {
    pub fn new(zones: Vec<Zone>) -> Self {
        Self(zones)
    }

    /// The zone with the longest apex containing `name` (case-insensitive).
    fn find(&self, name: &Name) -> Option<&Zone> {
        self.0
            .iter()
            .filter(|z| z.apex.zone_of(name))
            .max_by_key(|z| z.apex.iter().count())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// Nodes matching the zone's own `seed_flags`.
    Plain,
    /// `x<hex>`: nodes whose services are a superset of the mask. `x0` parses
    /// as `Plain`.
    Filter(u64),
}

/// Classify a name inside `apex`: the apex itself is `Plain`; exactly one
/// label `x<1..=16 hex digits>` (any case) is `Filter`; anything else is `None`.
pub fn parse_target(qname: &Name, apex: &Name) -> Option<Target> {
    if !apex.zone_of(qname) {
        return None;
    }
    // `Name::num_labels` skips a leading `*`; count real labels.
    if qname.iter().count() != apex.iter().count() + 1 {
        return None;
    }
    let label = qname.iter().next()?;
    let (&first, hex) = label.split_first()?;
    if !first.eq_ignore_ascii_case(&b'x') || hex.is_empty() || hex.len() > 16 {
        return None;
    }
    if !hex.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let text = std::str::from_utf8(hex).ok()?;
    // `x0` is the plain name: it never widens past the zone's seed flags.
    match u64::from_str_radix(text, 16).ok()? {
        0 => Some(Target::Plain),
        f => Some(Target::Filter(f)),
    }
}

fn soa(zone: &Zone, snap: &Snapshot) -> Record {
    let serial = (snap.built_at / 60).clamp(0, i64::from(u32::MAX)) as u32;
    let rdata = SOA::new(
        zone.nameserver.clone(),
        zone.mailbox.clone(),
        serial,
        SOA_REFRESH,
        SOA_RETRY,
        SOA_EXPIRE,
        SOA_MINIMUM,
    );
    Record::from_rdata(zone.apex.clone(), TTL_SOA, RData::SOA(rdata))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    V4,
    V6,
}

/// At most this many addresses of one network group in any answer or published name. A node
/// makes one outbound connection per group, so more would be wasted choices.
pub const MAX_PER_GROUP: usize = 2;

/// The network group of an address: the /16 of an IPv4 address (`[a, b, 0, 0]`), the /32 of an
/// IPv6 address. Compare groups only within one family.
pub fn netgroup(ip: &IpAddr) -> [u8; 4] {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            [o[0], o[1], 0, 0]
        }
        IpAddr::V6(v6) => {
            let o = v6.octets();
            [o[0], o[1], o[2], o[3]]
        }
    }
}

/// Every address of `family` whose node has services ⊇ the target's mask (`Plain` uses
/// `seed_flags`), in snapshot order.
pub fn matching(
    nodes: &[Servable],
    target: &Target,
    seed_flags: u64,
    family: Family,
) -> Vec<IpAddr> {
    let mask = match target {
        Target::Plain => seed_flags,
        Target::Filter(f) => *f,
    };
    nodes
        .iter()
        .filter(|n| n.services & mask == mask)
        .map(|n| n.addr)
        .filter(|a: &SocketAddr| a.is_ipv6() == (family == Family::V6))
        .map(|a| a.ip())
        .collect()
}

/// The addresses an answer for `target` may contain: services ⊇ filter
/// (`Plain` uses `seed_flags`), matching `family`, a random subset of at most `cap` with at
/// most [`MAX_PER_GROUP`] per network group.
pub fn select(
    nodes: &[Servable],
    target: &Target,
    seed_flags: u64,
    family: Family,
    cap: usize,
    rng: &mut impl Rng,
) -> Vec<IpAddr> {
    select_with(
        nodes,
        target,
        seed_flags,
        family,
        cap,
        &[],
        &HashSet::new(),
        rng,
    )
}

/// Like [`select`], but `already` addresses are held: it returns only NEW addresses (never in
/// `already` or `exclude`) such that `already` plus the result respects the group limit and
/// `already.len() + result.len() <= cap`. Strict: fewer are returned when the groups run out.
#[allow(clippy::too_many_arguments)]
pub fn select_with(
    nodes: &[Servable],
    target: &Target,
    seed_flags: u64,
    family: Family,
    cap: usize,
    already: &[IpAddr],
    exclude: &HashSet<IpAddr>,
    rng: &mut impl Rng,
) -> Vec<IpAddr> {
    let mut ips = matching(nodes, target, seed_flags, family);
    ips.shuffle(rng);
    let mut per_group: HashMap<[u8; 4], usize> = HashMap::new();
    for ip in already {
        *per_group.entry(netgroup(ip)).or_default() += 1;
    }
    let mut taken: HashSet<IpAddr> = already.iter().copied().collect();
    let room = cap.saturating_sub(already.len());
    let mut out = Vec::new();
    for ip in ips {
        if out.len() >= room {
            break;
        }
        if exclude.contains(&ip) || taken.contains(&ip) {
            continue;
        }
        let n = per_group.entry(netgroup(&ip)).or_default();
        if *n >= MAX_PER_GROUP {
            continue;
        }
        *n += 1;
        taken.insert(ip);
        out.push(ip);
    }
    out
}

/// Every name an enumerating publisher must publish for a zone: the apex
/// (`Plain`) and `x<hex>` for each filter, skipping 0 and duplicates.
pub fn published_names(apex: &Name, filters: &[u64]) -> Vec<(Name, Target)> {
    let mut out = vec![(apex.clone(), Target::Plain)];
    let mut seen = Vec::new();
    for &f in filters {
        if f == 0 || seen.contains(&f) {
            continue;
        }
        seen.push(f);
        if let Ok(label) = Name::from_ascii(format!("x{f:x}")).and_then(|l| l.append_domain(apex)) {
            out.push((label, Target::Filter(f)));
        }
    }
    out
}

/// Build the full response to one question. Pure apart from `rng`. The
/// message id is left 0 and recursion_desired false; the transport fills them.
pub fn build_answer(zones: &ZoneSet, query: &Query, edns: bool, rng: &mut impl Rng) -> Message {
    let mut msg = Message::new(0, MessageType::Response, OpCode::Query);
    msg.add_query(query.clone());
    if edns {
        let mut e = Edns::new();
        e.set_max_payload(ADVERTISED_PAYLOAD);
        msg.set_edns(e);
    }
    let qname = query.name();
    let qtype = query.query_type();
    let zone = zones.find(qname);
    let (Some(zone), DNSClass::IN, false) = (
        zone,
        query.query_class(),
        matches!(qtype, RecordType::AXFR | RecordType::IXFR),
    ) else {
        msg.metadata.response_code = ResponseCode::Refused;
        return msg;
    };
    msg.metadata.authoritative = true;
    let snap: Arc<Snapshot> = zone.snapshot.borrow().clone();

    if zone.apex.iter().count() == qname.iter().count() {
        match qtype {
            RecordType::NS => {
                msg.add_answer(Record::from_rdata(
                    qname.clone(),
                    TTL_NS,
                    RData::NS(NS(zone.nameserver.clone())),
                ));
            }
            RecordType::SOA => {
                let mut r = soa(zone, &snap);
                r.name = qname.clone();
                msg.add_answer(r);
            }
            RecordType::A | RecordType::AAAA => {
                answer_addrs(
                    &mut msg,
                    zone,
                    &snap,
                    Target::Plain,
                    qtype,
                    qname,
                    edns,
                    rng,
                );
            }
            _ => {
                msg.add_authority(soa(zone, &snap));
            }
        }
        return msg;
    }

    match parse_target(qname, &zone.apex) {
        None => {
            msg.metadata.response_code = ResponseCode::NXDomain;
            msg.add_authority(soa(zone, &snap));
        }
        Some(target) => {
            answer_addrs(&mut msg, zone, &snap, target, qtype, qname, edns, rng);
        }
    }
    msg
}

#[allow(clippy::too_many_arguments)]
fn answer_addrs(
    msg: &mut Message,
    zone: &Zone,
    snap: &Snapshot,
    target: Target,
    qtype: RecordType,
    qname: &Name,
    edns: bool,
    rng: &mut impl Rng,
) {
    let (family, cap) = match qtype {
        RecordType::A => (Family::V4, MAX_A),
        RecordType::AAAA => (
            Family::V6,
            if edns { MAX_AAAA_EDNS } else { MAX_AAAA_PLAIN },
        ),
        _ => {
            msg.add_authority(soa(zone, snap));
            return;
        }
    };
    let ips = select(&snap.nodes, &target, zone.seed_flags, family, cap, rng);
    if ips.is_empty() {
        msg.add_authority(soa(zone, snap));
        return;
    }
    for ip in ips {
        let rdata = match ip {
            IpAddr::V4(v4) => RData::A(A(v4)),
            IpAddr::V6(v6) => RData::AAAA(AAAA(v6)),
        };
        msg.add_answer(Record::from_rdata(qname.clone(), TTL_ADDR, rdata));
    }
}

#[cfg(test)]
mod tests {

    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use tokio::sync::watch;

    use super::*;

    const APEX: &str = "dnsseed.osmium.space.";

    fn name(s: &str) -> Name {
        Name::from_ascii(s).unwrap()
    }

    fn v4(i: u8, services: u64) -> Servable {
        Servable {
            addr: format!("203.0.113.{i}:23222").parse().unwrap(),
            services,
        }
    }

    /// Host `host` in the /16 `203.<group>.0.0`.
    fn v4g(group: u8, host: u8, services: u64) -> Servable {
        Servable {
            addr: format!("203.{group}.0.{host}:23222").parse().unwrap(),
            services,
        }
    }

    /// Host `host` in the IPv6 /32 `2001:<group>::/32`.
    fn v6g(group: u16, host: u16, services: u64) -> Servable {
        Servable {
            addr: format!("[2001:{group:x}::{host:x}]:23222").parse().unwrap(),
            services,
        }
    }

    fn v6(i: u16, services: u64) -> Servable {
        Servable {
            addr: format!("[2001:db8::{i:x}]:23222").parse().unwrap(),
            services,
        }
    }

    fn zone(apex: &str, seed_flags: u64, nodes: Vec<Servable>) -> Zone {
        let (_tx, rx) = watch::channel(Arc::new(Snapshot {
            nodes,
            built_at: 6000,
        }));
        Zone {
            apex: name(apex),
            nameserver: name(&format!("ns.{apex}")),
            mailbox: name(&format!("hostmaster.{apex}")),
            seed_flags,
            snapshot: rx,
        }
    }

    fn one(nodes: Vec<Servable>, seed_flags: u64) -> ZoneSet {
        ZoneSet::new(vec![zone(APEX, seed_flags, nodes)])
    }

    fn ask(zs: &ZoneSet, qname: &str, t: RecordType, edns: bool) -> Message {
        let q = Query::query(name(qname), t);
        build_answer(zs, &q, edns, &mut StdRng::seed_from_u64(7))
    }

    fn a_ips(m: &Message) -> Vec<IpAddr> {
        m.answers.iter().filter_map(|r| r.data.ip_addr()).collect()
    }

    #[test]
    fn parse_x1_and_upper_x1() {
        let apex = name(APEX);
        assert_eq!(
            parse_target(&name("x1.dnsseed.osmium.space."), &apex),
            Some(Target::Filter(1))
        );
        assert_eq!(
            parse_target(&name("X1D.DnsSeed.Osmium.Space."), &apex),
            Some(Target::Filter(0x1d))
        );
        assert_eq!(
            parse_target(&name("x0.dnsseed.osmium.space."), &apex),
            Some(Target::Plain)
        );
        assert_eq!(
            parse_target(&name("xFFFFFFFFFFFFFFFF.dnsseed.osmium.space."), &apex),
            Some(Target::Filter(u64::MAX))
        );
    }

    #[test]
    fn parse_rejects_x_without_hex_and_17_digits() {
        let apex = name(APEX);
        for bad in [
            "x.dnsseed.osmium.space.",
            "xg.dnsseed.osmium.space.",
            "1.dnsseed.osmium.space.",
            "x00000000000000001.dnsseed.osmium.space.",
            "a.x1.dnsseed.osmium.space.",
            "dnsseed.osmium.space.",
            "x1.other.space.",
        ] {
            assert_eq!(parse_target(&name(bad), &apex), None, "{bad}");
        }
    }

    #[test]
    fn plain_uses_seed_flags() {
        let zs = one(vec![v4(1, 0x1), v4(2, 0x5), v4(3, 0x0)], 0x1);
        let m = ask(&zs, APEX, RecordType::A, false);
        assert_eq!(m.metadata.response_code, ResponseCode::NoError);
        let mut ips = a_ips(&m);
        ips.sort();
        assert_eq!(
            ips,
            vec![
                "203.0.113.1".parse::<IpAddr>().unwrap(),
                "203.0.113.2".parse().unwrap()
            ]
        );
    }

    #[test]
    fn x9_returns_nodata_when_no_witness_nodes() {
        let zs = one(vec![v4(1, 0x1), v4(2, 0x5)], 0x1);
        let m = ask(&zs, "x9.dnsseed.osmium.space.", RecordType::A, false);
        assert_eq!(m.metadata.response_code, ResponseCode::NoError);
        assert!(m.answers.is_empty());
        assert_eq!(m.authorities[0].record_type(), RecordType::SOA);
    }

    #[test]
    fn arbitrary_filter_x40d_superset_match() {
        let zs = one(
            vec![v4(1, 0x40d), v4(2, 0x1_40d), v4(3, 0x401), v4(4, 0x5)],
            0x1,
        );
        let m = ask(&zs, "x40d.dnsseed.osmium.space.", RecordType::A, false);
        let mut ips = a_ips(&m);
        ips.sort();
        assert_eq!(
            ips,
            vec![
                "203.0.113.1".parse::<IpAddr>().unwrap(),
                "203.0.113.2".parse().unwrap()
            ]
        );
    }

    #[test]
    fn x0_equals_plain_and_respects_seed_flags() {
        let zs = one(vec![v4(1, 0), v4(2, 8), v4(3, 1), v4(4, 3)], 0x1);
        let x0 = ask(&zs, "x0.dnsseed.osmium.space.", RecordType::A, false);
        let plain = ask(&zs, APEX, RecordType::A, false);
        let (mut a, mut b) = (a_ips(&x0), a_ips(&plain));
        a.sort();
        b.sort();
        assert_eq!(a, b);
        assert_eq!(a.len(), 2);
    }

    #[test]
    fn a_caps_at_20() {
        let nodes: Vec<_> = (1..=50).map(|i| v4g(i, 1, 1)).collect();
        let zs = one(nodes, 1);
        let m = ask(&zs, "x1.dnsseed.osmium.space.", RecordType::A, false);
        assert_eq!(m.answers.len(), 20);
        let mut ips = a_ips(&m);
        ips.sort();
        ips.dedup();
        assert_eq!(ips.len(), 20);
        assert!(m.answers.iter().all(|r| r.ttl == 60));
    }

    #[test]
    fn aaaa_caps_at_15_without_edns_and_20_with() {
        let nodes: Vec<_> = (1..=40).map(|i| v6g(i, 1, 1)).collect();
        let zs = one(nodes, 1);
        let q = "x1.dnsseed.osmium.space.";
        assert_eq!(ask(&zs, q, RecordType::AAAA, false).answers.len(), 15);
        let m = ask(&zs, q, RecordType::AAAA, true);
        assert_eq!(m.answers.len(), 20);
        assert!(m.edns.is_some());
        assert!(ask(&zs, q, RecordType::AAAA, false).edns.is_none());
    }

    #[test]
    fn a_gets_only_v4_and_aaaa_only_v6() {
        let zs = one(vec![v4(1, 1), v6(1, 1)], 1);
        let a = ask(&zs, APEX, RecordType::A, false);
        assert!(a_ips(&a).iter().all(IpAddr::is_ipv4) && a.answers.len() == 1);
        let b = ask(&zs, APEX, RecordType::AAAA, false);
        assert!(a_ips(&b).iter().all(IpAddr::is_ipv6) && b.answers.len() == 1);
    }

    #[test]
    fn empty_snapshot_is_noerror_with_soa() {
        let zs = one(vec![], 1);
        for q in [APEX, "x1.dnsseed.osmium.space."] {
            for t in [RecordType::A, RecordType::AAAA] {
                let m = ask(&zs, q, t, true);
                assert_eq!(m.metadata.response_code, ResponseCode::NoError);
                assert!(m.metadata.authoritative);
                assert!(m.answers.is_empty());
                assert_eq!(m.authorities.len(), 1);
                assert_eq!(m.authorities[0].record_type(), RecordType::SOA);
            }
        }
    }

    #[test]
    fn unknown_label_is_nxdomain_with_soa() {
        let zs = one(vec![v4(1, 1)], 1);
        for q in [
            "foo.dnsseed.osmium.space.",
            "a.x1.dnsseed.osmium.space.",
            "*.dnsseed.osmium.space.",
            "*.x1.dnsseed.osmium.space.",
        ] {
            let m = ask(&zs, q, RecordType::A, false);
            assert_eq!(m.metadata.response_code, ResponseCode::NXDomain, "{q}");
            assert!(m.metadata.authoritative);
            assert_eq!(m.authorities[0].record_type(), RecordType::SOA);
        }
    }

    #[test]
    fn outside_zone_is_refused() {
        let zs = one(vec![v4(1, 1)], 1);
        let m = ask(&zs, "example.com.", RecordType::A, false);
        assert_eq!(m.metadata.response_code, ResponseCode::Refused);
        assert!(m.answers.is_empty());
        assert_eq!(m.queries.len(), 1);
    }

    #[test]
    fn mx_at_apex_is_nodata() {
        let zs = one(vec![v4(1, 1)], 1);
        let m = ask(&zs, APEX, RecordType::MX, false);
        assert_eq!(m.metadata.response_code, ResponseCode::NoError);
        assert!(m.answers.is_empty());
        assert_eq!(m.authorities[0].record_type(), RecordType::SOA);
        let m = ask(&zs, "x1.dnsseed.osmium.space.", RecordType::TXT, false);
        assert_eq!(m.metadata.response_code, ResponseCode::NoError);
        assert!(m.answers.is_empty());
    }

    #[test]
    fn ns_and_soa_at_apex() {
        let zs = one(vec![], 1);
        let ns = ask(&zs, APEX, RecordType::NS, false);
        assert_eq!(ns.answers.len(), 1);
        assert_eq!(ns.answers[0].ttl, 86_400);
        match &ns.answers[0].data {
            RData::NS(n) => assert_eq!(n.0, name("ns.dnsseed.osmium.space.")),
            other => panic!("{other:?}"),
        }
        let s = ask(&zs, APEX, RecordType::SOA, false);
        assert_eq!(s.answers.len(), 1);
        assert_eq!(s.answers[0].ttl, 900);
        match &s.answers[0].data {
            RData::SOA(s) => {
                assert_eq!(s.serial, 100);
                assert_eq!(s.refresh, 3600);
                assert_eq!(s.retry, 600);
                assert_eq!(s.expire, 604_800);
                assert_eq!(s.minimum, 60);
                assert_eq!(s.mname, name("ns.dnsseed.osmium.space."));
                assert_eq!(s.rname, name("hostmaster.dnsseed.osmium.space."));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn question_case_is_echoed() {
        let zs = one(vec![v4(1, 1)], 1);
        let qn = name("X1.DnSsEeD.OsMiUm.SpAcE.");
        let q = Query::query(qn.clone(), RecordType::A);
        let m = build_answer(&zs, &q, false, &mut StdRng::seed_from_u64(1));
        assert_eq!(m.answers.len(), 1);
        assert_eq!(m.queries[0].name().to_ascii(), "X1.DnSsEeD.OsMiUm.SpAcE.");
        assert!(m.queries[0].name().eq_case(&qn));
        assert!(m.answers[0].name.eq_case(&qn));
        let nx = ask(&zs, "FoO.DnSsEeD.OsMiUm.SpAcE.", RecordType::A, false);
        assert_eq!(nx.queries[0].name().to_ascii(), "FoO.DnSsEeD.OsMiUm.SpAcE.");
    }

    #[test]
    fn two_zones_answer_independently() {
        let zs = ZoneSet::new(vec![
            zone(APEX, 1, vec![v4(1, 1)]),
            zone("seed.other.example.", 2, vec![v4(9, 2)]),
        ]);
        let a = ask(&zs, APEX, RecordType::A, false);
        assert_eq!(a_ips(&a), vec!["203.0.113.1".parse::<IpAddr>().unwrap()]);
        let b = ask(&zs, "seed.other.example.", RecordType::A, false);
        assert_eq!(a_ips(&b), vec!["203.0.113.9".parse::<IpAddr>().unwrap()]);
        let c = ask(&zs, "x2.seed.other.example.", RecordType::A, false);
        assert_eq!(a_ips(&c).len(), 1);
    }

    #[test]
    fn answers_are_randomised_per_rng_seed() {
        let nodes: Vec<_> = (1..=50).map(|i| v4g(i, 1, 1)).collect();
        let zs = one(nodes, 1);
        let q = Query::query(name("x1.dnsseed.osmium.space."), RecordType::A);
        let set = |seed: u64| {
            let m = build_answer(&zs, &q, false, &mut StdRng::seed_from_u64(seed));
            let mut v = a_ips(&m);
            v.sort();
            v
        };
        assert_eq!(set(1), set(1));
        assert_ne!(set(1), set(2));
    }

    #[test]
    fn transfers_and_non_in_class_are_refused() {
        let zs = one(vec![v4(1, 1)], 1);
        for t in [RecordType::AXFR, RecordType::IXFR] {
            for q in [APEX, "x1.dnsseed.osmium.space."] {
                let m = ask(&zs, q, t, false);
                assert_eq!(m.metadata.response_code, ResponseCode::Refused);
                assert!(!m.metadata.authoritative);
                assert!(m.answers.is_empty() && m.authorities.is_empty());
            }
        }
        let mut q = Query::query(name("x1.dnsseed.osmium.space."), RecordType::A);
        q.set_query_class(DNSClass::CH);
        let m = build_answer(&zs, &q, false, &mut StdRng::seed_from_u64(1));
        assert_eq!(m.metadata.response_code, ResponseCode::Refused);
        assert!(!m.metadata.authoritative);
        assert!(m.answers.is_empty() && m.authorities.is_empty());
    }

    #[test]
    fn select_matches_build_answer() {
        let nodes: Vec<_> = (1..=50).map(|i| v4g(i, 1, 1)).collect();
        let zs = one(nodes.clone(), 1);
        let q = Query::query(name("x1.dnsseed.osmium.space."), RecordType::A);
        let m = build_answer(&zs, &q, false, &mut StdRng::seed_from_u64(9));
        let got = select(
            &nodes,
            &Target::Filter(1),
            1,
            Family::V4,
            20,
            &mut StdRng::seed_from_u64(9),
        );
        assert_eq!(a_ips(&m), got);
        assert_eq!(got.len(), 20);
    }

    fn sel(nodes: &[Servable], family: Family, seed: u64) -> Vec<IpAddr> {
        select(
            nodes,
            &Target::Filter(1),
            1,
            family,
            20,
            &mut StdRng::seed_from_u64(seed),
        )
    }

    fn groups(ips: &[IpAddr]) -> HashMap<[u8; 4], usize> {
        let mut m = HashMap::new();
        for ip in ips {
            *m.entry(netgroup(ip)).or_default() += 1;
        }
        m
    }

    #[test]
    fn netgroup_v4_is_the_slash_16() {
        let g = |s: &str| netgroup(&s.parse().unwrap());
        assert_eq!(g("151.244.3.4"), [151, 244, 0, 0]);
        assert_eq!(g("151.244.3.4"), g("151.244.200.9"));
        assert_ne!(g("151.244.3.4"), g("151.245.3.4"));
        assert_ne!(g("151.244.3.4"), g("152.244.3.4"));
    }

    #[test]
    fn netgroup_v6_is_the_slash_32() {
        let g = |s: &str| netgroup(&s.parse().unwrap());
        assert_eq!(g("2001:db8:1::1"), [0x20, 0x01, 0x0d, 0xb8]);
        assert_eq!(g("2001:db8:1::1"), g("2001:db8:ffff:2::9"));
        assert_ne!(g("2001:db8::1"), g("2001:db9::1"));
        assert_ne!(g("2001:db8::1"), g("2002:db8::1"));
    }

    #[test]
    fn fifty_nodes_in_one_slash_16_give_two() {
        let nodes: Vec<_> = (1..=50).map(|h| v4g(7, h, 1)).collect();
        let got = sel(&nodes, Family::V4, 3);
        assert_eq!(got.len(), MAX_PER_GROUP);
        assert_eq!(groups(&got).len(), 1);
    }

    #[test]
    fn thirty_spread_nodes_give_twenty() {
        let nodes: Vec<_> = (1..=30).map(|g| v4g(g, 1, 1)).collect();
        assert_eq!(sel(&nodes, Family::V4, 3).len(), 20);
    }

    #[test]
    fn a_big_group_is_limited_and_the_rest_fill() {
        let mut nodes: Vec<_> = (1..=20).map(|h| v4g(200, h, 1)).collect();
        nodes.extend((1..=10).map(|g| v4g(g, 1, 1)));
        for seed in 0..20 {
            let got = sel(&nodes, Family::V4, seed);
            assert_eq!(got.len(), 12, "seed {seed}");
            assert_eq!(groups(&got)[&[203, 200, 0, 0]], 2);
        }
    }

    #[test]
    fn v6_groups_are_limited_too() {
        let mut nodes: Vec<_> = (1..=10).map(|h| v6g(0xaaa, h, 1)).collect();
        nodes.extend((1..=3).map(|g| v6g(g, 1, 1)));
        let got = sel(&nodes, Family::V6, 1);
        assert_eq!(got.len(), 5);
    }

    #[test]
    fn select_with_counts_the_addresses_already_held() {
        let nodes: Vec<_> = (1..=5)
            .map(|h| v4g(7, h, 1))
            .chain((1..=5).map(|g| v4g(g, 9, 1)))
            .collect();
        let held: Vec<IpAddr> = vec!["203.7.0.1".parse().unwrap()];
        let mut exclude = HashSet::new();
        exclude.insert("203.1.0.9".parse::<IpAddr>().unwrap());
        for seed in 0..20 {
            let got = select_with(
                &nodes,
                &Target::Filter(1),
                1,
                Family::V4,
                4,
                &held,
                &exclude,
                &mut StdRng::seed_from_u64(seed),
            );
            assert_eq!(got.len(), 3, "cap 4 minus one held");
            assert!(!got.contains(&held[0]) && !got.contains(&exclude_one(&exclude)));
            let mut all = held.clone();
            all.extend(&got);
            assert!(groups(&all).values().all(|&n| n <= MAX_PER_GROUP));
        }
    }

    fn exclude_one(e: &HashSet<IpAddr>) -> IpAddr {
        *e.iter().next().unwrap()
    }

    #[test]
    fn select_with_is_strict_when_held_groups_are_full() {
        let nodes: Vec<_> = (1..=5).map(|h| v4g(7, h, 1)).collect();
        let held: Vec<IpAddr> = vec!["203.7.0.1".parse().unwrap(), "203.7.0.2".parse().unwrap()];
        let got = select_with(
            &nodes,
            &Target::Filter(1),
            1,
            Family::V4,
            20,
            &held,
            &HashSet::new(),
            &mut StdRng::seed_from_u64(1),
        );
        assert!(got.is_empty());
    }

    #[test]
    fn authoritative_answers_are_group_limited() {
        let nodes: Vec<_> = (1..=50).map(|h| v4g(7, h, 1)).collect();
        let zs = one(nodes, 1);
        let m = ask(&zs, "x1.dnsseed.osmium.space.", RecordType::A, false);
        assert_eq!(m.answers.len(), 2);
    }

    #[test]
    fn published_names_for_osmium() {
        let got = published_names(&name(APEX), &[0x1, 0x5]);
        assert_eq!(
            got,
            vec![
                (name(APEX), Target::Plain),
                (name("x1.dnsseed.osmium.space."), Target::Filter(1)),
                (name("x5.dnsseed.osmium.space."), Target::Filter(5)),
            ]
        );
    }

    #[test]
    fn published_names_skip_x0_and_dedupe() {
        let got = published_names(&name(APEX), &[0x0, 0x1, 0x1]);
        assert_eq!(
            got,
            vec![
                (name(APEX), Target::Plain),
                (name("x1.dnsseed.osmium.space."), Target::Filter(1)),
            ]
        );
    }

    #[test]
    fn published_names_use_lowercase_hex_labels() {
        let apex = name(APEX);
        let got = published_names(&apex, &[0x1, 0xd, 0x449]);
        let ascii: Vec<String> = got.iter().map(|(n, _)| n.to_ascii()).collect();
        assert_eq!(
            ascii,
            vec![
                "dnsseed.osmium.space.",
                "x1.dnsseed.osmium.space.",
                "xd.dnsseed.osmium.space.",
                "x449.dnsseed.osmium.space.",
            ]
        );
        for ((n, target), filter) in got.iter().skip(1).zip([0x1, 0xd, 0x449]) {
            assert_eq!(*target, Target::Filter(filter));
            assert_eq!(parse_target(n, &apex), Some(Target::Filter(filter)));
        }
    }

    #[test]
    fn the_longest_matching_zone_answers_nested_apexes() {
        let zs = ZoneSet::new(vec![
            zone("seed.example.", 1, vec![v4(1, 1)]),
            zone("t.seed.example.", 1, vec![v4(2, 1)]),
        ]);
        let inner = "203.0.113.2".parse::<IpAddr>().unwrap();
        for q in ["t.seed.example.", "x1.t.seed.example."] {
            let m = ask(&zs, q, RecordType::A, false);
            assert_eq!(a_ips(&m), vec![inner], "{q}");
        }
        let outer = ask(&zs, "x1.seed.example.", RecordType::A, false);
        assert_eq!(
            a_ips(&outer),
            vec!["203.0.113.1".parse::<IpAddr>().unwrap()]
        );
        let m = ask(&zs, "x1.t.seed.example.", RecordType::MX, false);
        assert!(m.answers.is_empty());
        assert_eq!(m.authorities.len(), 1);
        assert_eq!(m.authorities[0].name.to_ascii(), "t.seed.example.");
        match &m.authorities[0].data {
            RData::SOA(s) => assert_eq!(s.mname, name("ns.t.seed.example.")),
            other => panic!("{other:?}"),
        }
    }
}
