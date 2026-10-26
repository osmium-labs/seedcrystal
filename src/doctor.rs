//! `seedcrystal doctor`: checks a seed from the outside, the way a coin node resolves it.
//!
//! The four checks follow the path of a real lookup: is the seed delegated here (or are its
//! records in the Cloudflare zone), does the exact name the coin's node queries resolve,
//! are the addresses it returns real nodes, and are real resolvers asking.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{Name, RData, RecordType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::cloudflare::api::{ApiError, Client, OWNER_COMMENT, normalize_name};
use crate::coin::CoinPreset;
use crate::config::{self, NetworkConfig, Publish};
use crate::dns::answer::published_names;
use crate::dns::querylog;
use crate::p2p::probe::{ProbeOptions, probe};

/// How recent a query must be to count as "resolvers are asking".
const FRESH_SECS: u64 = 24 * 3600;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Pass,
    Fail,
    Skip,
}

#[derive(Debug, Clone)]
pub struct Check {
    pub step: u8,
    pub name: String,
    pub status: Status,
    pub detail: String,
    pub fix: Option<String>,
}

impl fmt::Display for Check {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tag = match self.status {
            Status::Pass => "PASS",
            Status::Fail => "FAIL",
            Status::Skip => "SKIP",
        };
        write!(
            f,
            "{tag}  {}  {} \u{2014} {}",
            self.step, self.name, self.detail
        )?;
        if let Some(fix) = &self.fix {
            write!(f, "\n  fix: {fix}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DoctorOptions {
    pub resolvers: Vec<SocketAddr>,
    /// Per query, UDP and any TCP retry together.
    pub timeout: Duration,
    /// Most distinct addresses handshaken in step 3.
    pub probe_max: usize,
}

impl Default for DoctorOptions {
    fn default() -> Self {
        Self {
            resolvers: ["1.1.1.1:53", "8.8.8.8:53", "9.9.9.9:53"]
                .iter()
                .map(|s| s.parse().expect("literal address"))
                .collect(),
            timeout: Duration::from_secs(3),
            probe_max: 5,
        }
    }
}

/// Runs the four checks for one network. Always returns one entry per step, in order.
pub async fn run_checks(
    net: &NetworkConfig,
    preset: &CoinPreset,
    opts: &DoctorOptions,
) -> Vec<Check> {
    let names = Names::new(net, preset);
    let step1 = match net.publish {
        Publish::Authoritative => delegation(net, &names, opts).await,
        Publish::Cloudflare => cloudflare_records(net, preset, &names).await,
    };
    let (step2, addrs) = resolution(&names, opts).await;
    let step3 = real_nodes(preset, &addrs, opts).await;
    let step4 = match net.publish {
        Publish::Authoritative => asking(net, preset, &names),
        Publish::Cloudflare => cloudflare_asking(net, preset, &names, querylog::unix_now()).await,
    };
    vec![step1, step2, step3, step4]
}

fn check(step: u8, name: &str, status: Status, detail: String, fix: Option<String>) -> Check {
    Check {
        step,
        name: name.to_string(),
        status,
        detail,
        fix,
    }
}

/// The names a node asks for, as text without a trailing dot.
struct Names {
    seed: String,
    /// `x<seed_flags>.<seed>`: what the coin's node queries. None when the flags are 0.
    x: Option<String>,
}

impl Names {
    fn new(net: &NetworkConfig, preset: &CoinPreset) -> Self {
        let seed = normalize_name(&net.seed);
        let x = (preset.seed_flags != 0).then(|| format!("x{:x}.{seed}", preset.seed_flags));
        Self { seed, x }
    }

    /// The node's name first: it is the one that matters.
    fn all(&self) -> Vec<&str> {
        self.x
            .iter()
            .map(String::as_str)
            .chain(std::iter::once(self.seed.as_str()))
            .collect()
    }
}

fn dns_name(text: &str) -> Result<Name, String> {
    config::fqdn(text).map_err(|e| format!("`{text}` is not a DNS name: {e}"))
}

// ---- the stub resolver -------------------------------------------------------------------

struct Reply {
    rcode: ResponseCode,
    answers: Vec<RData>,
}

fn rcode_text(rc: ResponseCode) -> String {
    match rc {
        ResponseCode::NoError => "NOERROR".into(),
        ResponseCode::NXDomain => "NXDOMAIN".into(),
        ResponseCode::ServFail => "SERVFAIL".into(),
        ResponseCode::Refused => "REFUSED".into(),
        ResponseCode::FormErr => "FORMERR".into(),
        ResponseCode::NotImp => "NOTIMP".into(),
        other => format!("{other:?}").to_ascii_uppercase(),
    }
}

/// One recursive query, as a stub resolver sends it: RD set, UDP first, TCP when truncated.
/// Err is a transport failure (timeout, unreachable, garbled), not a DNS answer.
async fn ask(
    resolver: SocketAddr,
    name: &Name,
    kind: RecordType,
    limit: Duration,
) -> Result<Reply, String> {
    let id: u16 = rand::random();
    let mut msg = Message::new(id, MessageType::Query, OpCode::Query);
    msg.metadata.recursion_desired = true;
    msg.add_query(Query::query(name.clone(), kind));
    let wire = msg.to_vec().map_err(|e| format!("encode query: {e}"))?;
    match timeout(limit, exchange(resolver, &wire, id, name, kind)).await {
        Ok(r) => r,
        Err(_) => Err(format!("no answer within {}s", limit.as_secs_f32())),
    }
}

fn matches_question(reply: &Message, id: u16, name: &Name, kind: RecordType) -> bool {
    reply.metadata.id == id
        && reply.metadata.message_type == MessageType::Response
        && reply.queries.first().is_some_and(|q| {
            q.query_type() == kind && q.name().to_ascii().eq_ignore_ascii_case(&name.to_ascii())
        })
}

async fn exchange(
    resolver: SocketAddr,
    wire: &[u8],
    id: u16,
    name: &Name,
    kind: RecordType,
) -> Result<Reply, String> {
    let bind = if resolver.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let sock = UdpSocket::bind(bind)
        .await
        .map_err(|e| format!("bind: {e}"))?;
    sock.connect(resolver)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    sock.send(wire).await.map_err(|e| format!("send: {e}"))?;
    let mut buf = vec![0u8; 4096];
    let reply = loop {
        let n = sock
            .recv(&mut buf)
            .await
            .map_err(|e| format!("recv: {e}"))?;
        // A stray or spoofed datagram is ignored; keep waiting for ours.
        if let Ok(m) = Message::from_vec(&buf[..n])
            && matches_question(&m, id, name, kind)
        {
            break m;
        }
    };
    let reply = if reply.metadata.truncation {
        over_tcp(resolver, wire, id, name, kind).await?
    } else {
        reply
    };
    Ok(Reply {
        rcode: reply.metadata.response_code,
        answers: reply.answers.into_iter().map(|r| r.data).collect(),
    })
}

async fn over_tcp(
    resolver: SocketAddr,
    wire: &[u8],
    id: u16,
    name: &Name,
    kind: RecordType,
) -> Result<Message, String> {
    let mut s = TcpStream::connect(resolver)
        .await
        .map_err(|e| format!("tcp connect: {e}"))?;
    let len = u16::try_from(wire.len()).map_err(|_| "query too long".to_string())?;
    let mut framed = len.to_be_bytes().to_vec();
    framed.extend_from_slice(wire);
    s.write_all(&framed)
        .await
        .map_err(|e| format!("tcp send: {e}"))?;
    let mut len = [0u8; 2];
    s.read_exact(&mut len)
        .await
        .map_err(|e| format!("tcp recv: {e}"))?;
    let mut body = vec![0u8; usize::from(u16::from_be_bytes(len))];
    s.read_exact(&mut body)
        .await
        .map_err(|e| format!("tcp recv: {e}"))?;
    let m = Message::from_vec(&body).map_err(|e| format!("tcp reply: {e}"))?;
    if matches_question(&m, id, name, kind) {
        Ok(m)
    } else {
        Err("tcp reply does not match the question".into())
    }
}

/// Runs `f` for every resolver at once; results come back in resolver order. A task that
/// panics becomes `failed(reason)` for its resolver; it never hides the others.
async fn per_resolver<T, F, Fut>(
    resolvers: &[SocketAddr],
    failed: fn(String) -> T,
    f: F,
) -> Vec<(SocketAddr, T)>
where
    T: Send + 'static,
    F: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = T> + Send + 'static,
{
    let mut set = JoinSet::new();
    let mut by_task = HashMap::new();
    for (i, &r) in resolvers.iter().enumerate() {
        let fut = f(r);
        let handle = set.spawn(fut);
        by_task.insert(handle.id(), i);
    }
    let mut out = Vec::new();
    while let Some(joined) = set.join_next_with_id().await {
        match joined {
            Ok((id, t)) => out.push((by_task[&id], t)),
            Err(e) => out.push((by_task[&e.id()], failed("check task failed".into()))),
        }
    }
    out.sort_by_key(|(i, _)| *i);
    out.into_iter().map(|(i, t)| (resolvers[i], t)).collect()
}

fn list(addrs: &[SocketAddr]) -> String {
    addrs
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn silent_note(silent: &[(SocketAddr, String)]) -> String {
    if silent.is_empty() {
        return String::new();
    }
    let each = silent
        .iter()
        .map(|(r, why)| format!("{r}: {why}"))
        .collect::<Vec<_>>()
        .join("; ");
    format!(" (no usable answer from {each})")
}

const NO_RESOLVER_FIX: &str = "no resolver answered: check this machine's outbound DNS \
                               (UDP and TCP port 53), or pass a reachable one with --resolver";

// ---- step 1: delegation ------------------------------------------------------------------

/// One resolver's view of the delegation.
enum Delegation {
    /// Carries the other NS names the resolver also returned.
    Ok(Vec<String>),
    Bad(String),
    Silent(String),
}

async fn delegation(net: &NetworkConfig, names: &Names, opts: &DoctorOptions) -> Check {
    const NAME: &str = "delegation";
    let (Some(ns_text), Ok(seed), limit) = (&net.nameserver, dns_name(&names.seed), opts.timeout)
    else {
        return check(
            1,
            NAME,
            Status::Fail,
            "the network has no `nameserver` configured".into(),
            Some("set `nameserver` in the [[network]] table".into()),
        );
    };
    let ns_text = normalize_name(ns_text);
    let Ok(ns_name) = dns_name(&ns_text) else {
        return check(
            1,
            NAME,
            Status::Fail,
            format!("bad nameserver `{ns_text}`"),
            None,
        );
    };
    let ctx = Arc::new((seed, ns_name, ns_text.clone()));
    let results = per_resolver(&opts.resolvers, Delegation::Silent, |r| {
        let ctx = ctx.clone();
        async move {
            let (seed, ns_name, ns_text) = &*ctx;
            let seed_text = normalize_name(&seed.to_ascii());
            let reply = match ask(r, seed, RecordType::NS, limit).await {
                Ok(reply) => reply,
                Err(e) => return Delegation::Silent(e),
            };
            let theirs: Vec<String> = reply
                .answers
                .iter()
                .filter_map(|d| match d {
                    RData::NS(ns) => Some(normalize_name(&ns.0.to_ascii())),
                    _ => None,
                })
                .collect();
            let cname = reply.answers.iter().find_map(|d| match d {
                RData::CNAME(c) => Some(normalize_name(&c.0.to_ascii())),
                _ => None,
            });
            if let Some(target) = cname {
                return Delegation::Bad(format!(
                    "{seed_text} is a CNAME to {target} \u{2014} a seed name must be \
                     delegated with NS"
                ));
            }
            if reply.rcode != ResponseCode::NoError {
                return Delegation::Bad(format!(
                    "NS lookup returns {}, expected {ns_text}",
                    rcode_text(reply.rcode)
                ));
            }
            if theirs.is_empty() {
                return Delegation::Bad(format!("no NS record for {seed_text} (NODATA)"));
            }
            if !theirs.contains(ns_text) {
                return Delegation::Bad(format!("NS is {}, not {ns_text}", theirs.join(", ")));
            }
            let extra: Vec<String> = theirs.iter().filter(|n| *n != ns_text).cloned().collect();
            for kind in [RecordType::A, RecordType::AAAA] {
                match ask(r, ns_name, kind, limit).await {
                    Ok(a) if a.answers.iter().any(|d| addr_of(d).is_some()) => {
                        return Delegation::Ok(extra);
                    }
                    Ok(_) => {}
                    Err(e) => return Delegation::Silent(e),
                }
            }
            Delegation::Bad(format!("the nameserver {ns_text} has no A or AAAA record"))
        }
    })
    .await;

    let mut bad = Vec::new();
    let mut silent = Vec::new();
    let mut good = 0usize;
    let mut also: Vec<String> = Vec::new();
    for (r, v) in results {
        match v {
            Delegation::Ok(extra) => {
                good += 1;
                for n in extra {
                    if !also.contains(&n) {
                        also.push(n);
                    }
                }
            }
            Delegation::Bad(why) => bad.push((r, why)),
            Delegation::Silent(why) => silent.push((r, why)),
        }
    }
    let answered = good + bad.len();
    if answered == 0 {
        return check(
            1,
            NAME,
            Status::Fail,
            format!("no resolver answered{}", silent_note(&silent)),
            Some(NO_RESOLVER_FIX.into()),
        );
    }
    if bad.is_empty() {
        return check(
            1,
            NAME,
            Status::Pass,
            format!(
                "{} is delegated to {ns_text}, which resolves, at {good} resolver(s){}{}",
                names.seed,
                also_note(&also),
                silent_note(&silent)
            ),
            None,
        );
    }
    let detail = bad
        .iter()
        .map(|(r, why)| format!("{r}: {why}"))
        .collect::<Vec<_>>()
        .join("; ");
    check(
        1,
        NAME,
        Status::Fail,
        format!("{detail}{}", silent_note(&silent)),
        Some(format!(
            "delegate {} to {ns_text}: add an NS record for it in the parent zone at your \
             registrar or DNS host, and make sure {ns_text} has an A or AAAA record",
            names.seed
        )),
    )
}

fn also_note(also: &[String]) -> String {
    if also.is_empty() {
        String::new()
    } else {
        format!(
            "; also delegated to {} \u{2014} fine if it serves the same zone",
            also.join(", ")
        )
    }
}

fn addr_of(d: &RData) -> Option<IpAddr> {
    match d {
        RData::A(a) => Some(IpAddr::V4(a.0)),
        RData::AAAA(a) => Some(IpAddr::V6(a.0)),
        _ => None,
    }
}

async fn cloudflare_records(net: &NetworkConfig, preset: &CoinPreset, names: &Names) -> Check {
    const NAME: &str = "records in the Cloudflare zone";
    let fail =
        |detail: String, fix: &str| check(1, NAME, Status::Fail, detail, Some(fix.to_string()));
    let Some(cf) = &net.cloudflare else {
        return fail(
            "no [network.cloudflare] table".into(),
            "add a [network.cloudflare] table with `zone_id`",
        );
    };
    let token_env = std::env::var(config::TOKEN_ENV).ok();
    let client = match config::resolve_token(cf, token_env.as_deref())
        .and_then(|t| Client::new(&cf.api_base, &cf.zone_id, t))
    {
        Ok(c) => c,
        Err(e) => return fail(format!("{e:#}"), "fix the Cloudflare token setup"),
    };
    let Ok(apex) = dns_name(&names.seed) else {
        return fail("bad seed name".into(), "check `seed`");
    };
    let published = published_names(&apex, &preset.filters);
    let mut owned = 0usize;
    let mut with_owned = 0usize;
    let mut foreign = Vec::new();
    for (name, _) in &published {
        let text = normalize_name(&name.to_ascii());
        let records = match client.records_for(&text).await {
            Ok(r) => r,
            Err(e @ ApiError::Zone(_)) => {
                return fail(
                    e.to_string(),
                    "copy the zone_id from the zone's Overview page (API section)",
                );
            }
            Err(e) => {
                return fail(
                    e.to_string(),
                    "check the zone_id and that the token has Zone > DNS > Edit on that zone",
                );
            }
        };
        let mine = records
            .iter()
            .filter(|r| r.comment.as_deref() == Some(OWNER_COMMENT))
            .count();
        owned += mine;
        with_owned += usize::from(mine > 0);
        for r in records
            .iter()
            .filter(|r| r.comment.as_deref() != Some(OWNER_COMMENT))
        {
            let note = r
                .comment
                .as_deref()
                .map_or(String::new(), |c| format!(" (comment `{c}`)"));
            foreign.push(format!("{} {:?} {}{note}", r.name, r.kind, r.content));
        }
    }
    let mut problems = Vec::new();
    let mut fixes = Vec::new();
    if !foreign.is_empty() {
        problems.push(format!("foreign records: {}", foreign.join("; ")));
        fixes.push(
            "delete them in the dashboard; seedcrystal never changes records it does not own, \
             and they are served to nodes alongside its own"
                .to_string(),
        );
    }
    if owned == 0 {
        problems
            .push("the zone holds no record owned by seedcrystal for any published name".into());
        fixes.push("is the seed name inside this zone, and has the seeder published yet?".into());
    }
    if problems.is_empty() {
        return check(
            1,
            NAME,
            Status::Pass,
            format!(
                "{owned} seedcrystal-owned record(s) on {with_owned} of {} published names",
                published.len()
            ),
            None,
        );
    }
    check(
        1,
        NAME,
        Status::Fail,
        problems.join("; "),
        Some(fixes.join("; ")),
    )
}

// ---- step 2: what a node resolves ----------------------------------------------------------

/// What one resolver said about the names, or why it said nothing.
#[derive(Default)]
struct View {
    /// (name, family, what is wrong)
    problems: Vec<(String, RecordType, String)>,
    addrs: Vec<IpAddr>,
    silent: Option<String>,
}

fn silent_view(why: String) -> View {
    View {
        silent: Some(why),
        ..View::default()
    }
}

async fn resolution(names: &Names, opts: &DoctorOptions) -> (Check, Vec<IpAddr>) {
    const NAME: &str = "what a node resolves";
    let limit = opts.timeout;
    let queries: Arc<Vec<(String, Name)>> = Arc::new(
        names
            .all()
            .into_iter()
            .filter_map(|n| Some((n.to_string(), dns_name(n).ok()?)))
            .collect(),
    );
    let views = per_resolver(&opts.resolvers, silent_view, |r| {
        let queries = queries.clone();
        async move {
            let mut view = View::default();
            for (text, name) in queries.iter() {
                for kind in [RecordType::A, RecordType::AAAA] {
                    let reply = match ask(r, name, kind, limit).await {
                        Ok(reply) => reply,
                        Err(e) => {
                            view.silent = Some(e);
                            return view;
                        }
                    };
                    let found: Vec<IpAddr> = reply.answers.iter().filter_map(addr_of).collect();
                    if reply.rcode != ResponseCode::NoError {
                        view.problems
                            .push((text.clone(), kind, rcode_text(reply.rcode)));
                    } else if found.is_empty() && kind == RecordType::A {
                        view.problems.push((text.clone(), kind, "NODATA".into()));
                    }
                    view.addrs.extend(found);
                }
            }
            view
        }
    })
    .await;

    let mut addrs: Vec<IpAddr> = Vec::new();
    let mut silent = Vec::new();
    let mut grouped: BTreeMap<(String, String), (Vec<String>, Vec<SocketAddr>)> = BTreeMap::new();
    let mut answered = 0usize;
    for (r, view) in views {
        if let Some(why) = view.silent {
            silent.push((r, why));
            continue;
        }
        answered += 1;
        for (name, kind, what) in view.problems {
            let (kinds, who) = grouped.entry((name, what)).or_default();
            if !kinds.contains(&format!("{kind}")) {
                kinds.push(format!("{kind}"));
            }
            if !who.contains(&r) {
                who.push(r);
            }
        }
        for a in view.addrs {
            if !addrs.contains(&a) {
                addrs.push(a);
            }
        }
    }
    if answered == 0 {
        return (
            check(
                2,
                NAME,
                Status::Fail,
                format!("no resolver answered{}", silent_note(&silent)),
                Some(NO_RESOLVER_FIX.into()),
            ),
            addrs,
        );
    }
    if grouped.is_empty() {
        let shown = names.all().join(" and ");
        return (
            check(
                2,
                NAME,
                Status::Pass,
                format!(
                    "{shown} resolve at {answered} resolver(s), {} distinct address(es){}",
                    addrs.len(),
                    silent_note(&silent)
                ),
                None,
            ),
            addrs,
        );
    }
    let mut detail = Vec::new();
    let mut fixes = Vec::new();
    for ((name, what), (kinds, resolvers)) in &grouped {
        let via = list(resolvers);
        detail.push(format!("{} {name} -> {what} via {via}", kinds.join("/")));
        let is_x = names.x.as_deref() == Some(name.as_str());
        fixes.push(match (is_x, what.as_str()) {
            (true, "NODATA") => format!(
                "your node queries `{name}`, which has no addresses: the seeder has no nodes \
                 for that filter yet (via {via})"
            ),
            (true, "NXDOMAIN") => format!(
                "your node queries `{name}`, which returns NXDOMAIN: publish that name (via {via})"
            ),
            (_, "SERVFAIL") => format!(
                "the resolver cannot get an answer from your nameserver: check step 1 and that \
                 the server is up and reachable on UDP and TCP 53 (via {via})"
            ),
            (_, "REFUSED") => {
                format!("that resolver refuses the query; try another with --resolver (via {via})")
            }
            (true, _) => format!("your node queries `{name}`, which returns {what} (via {via})"),
            (false, "NODATA") => {
                format!("`{name}` has no addresses: the seeder has no nodes yet (via {via})")
            }
            (false, "NXDOMAIN") => {
                format!("`{name}` returns NXDOMAIN: publish it (via {via})")
            }
            (false, _) => format!("`{name}` returns {what} (via {via})"),
        });
    }
    (
        check(
            2,
            NAME,
            Status::Fail,
            format!("{}{}", detail.join("; "), silent_note(&silent)),
            Some(fixes.join("; ")),
        ),
        addrs,
    )
}

// ---- step 3: the answers are real nodes ----------------------------------------------------

async fn real_nodes(preset: &CoinPreset, addrs: &[IpAddr], opts: &DoctorOptions) -> Check {
    const NAME: &str = "the answers are real nodes";
    let targets: Vec<SocketAddr> = addrs
        .iter()
        .take(opts.probe_max)
        .map(|ip| SocketAddr::new(*ip, preset.default_port))
        .collect();
    if targets.is_empty() {
        return check(3, NAME, Status::Skip, "no addresses to probe".into(), None);
    }
    let popts = ProbeOptions {
        want_addrs: false,
        ..ProbeOptions::default()
    };
    let mut set = JoinSet::new();
    let mut by_task = HashMap::new();
    for (i, &t) in targets.iter().enumerate() {
        let preset = preset.clone();
        let popts = popts.clone();
        let handle =
            set.spawn(async move { probe(t, &preset, &popts).await.map_err(|e| e.to_string()) });
        by_task.insert(handle.id(), i);
    }
    let mut results = Vec::new();
    while let Some(joined) = set.join_next_with_id().await {
        match joined {
            Ok((id, res)) => results.push((by_task[&id], res)),
            Err(e) => results.push((by_task[&e.id()], Err("probe task failed".to_string()))),
        }
    }
    results.sort_by_key(|(i, _)| *i);
    let mut ok = 0usize;
    let mut lines = Vec::new();
    for (i, res) in results {
        let t = targets[i];
        match res {
            Ok(r) => {
                ok += 1;
                lines.push(format!(
                    "{t} version {} height {}",
                    r.peer.version, r.peer.start_height
                ));
            }
            Err(e) => lines.push(format!("{t} failed: {e}")),
        }
    }
    let detail = format!(
        "{ok} of {} answered the handshake: {}",
        targets.len(),
        lines.join("; ")
    );
    if ok > 0 {
        return check(3, NAME, Status::Pass, detail, None);
    }
    check(
        3,
        NAME,
        Status::Fail,
        detail,
        Some(format!(
            "the addresses the seed returns are not {} nodes on port {} at version {} or \
             newer: check that the crawler finds real nodes and that the preset's port and \
             magic are right",
            preset.id, preset.default_port, preset.min_peer_version
        )),
    )
}

// ---- step 4: real resolvers are asking -------------------------------------------------------

fn asking(net: &NetworkConfig, preset: &CoinPreset, names: &Names) -> Check {
    const NAME: &str = "real resolvers are asking";
    let path = querylog::path_for(&net.data_dir, &preset.id);
    if !path.exists() {
        return check(
            4,
            NAME,
            Status::Fail,
            format!(
                "no query log yet at {} \u{2014} is the seeder running in authoritative mode here?",
                path.display()
            ),
            Some(
                "start `seedcrystal run` for this network; it writes the log once a minute".into(),
            ),
        );
    }
    let log = match querylog::read(&path) {
        Ok(l) => l,
        Err(e) => {
            return check(
                4,
                NAME,
                Status::Fail,
                format!("{e:#}"),
                Some("remove the file; the seeder rewrites it".into()),
            );
        }
    };
    let now = querylog::unix_now();
    let best = names
        .all()
        .into_iter()
        .filter_map(|n| log.names.get(n).map(|s| (n, s)))
        .filter(|(_, s)| s.answered > 0 && now.saturating_sub(s.last_seen) <= FRESH_SECS)
        .max_by_key(|(_, s)| s.last_seen);
    match best {
        Some((name, s)) => check(
            4,
            NAME,
            Status::Pass,
            format!(
                "{name} answered {} time(s), last {} ago; the count includes your own dig \
                 and doctor runs",
                s.answered,
                age(now.saturating_sub(s.last_seen))
            ),
            None,
        ),
        None => check(
            4,
            NAME,
            Status::Fail,
            format!(
                "no answered query for {} in the last 24 h (counting since unix {})",
                names.all().join(" or "),
                log.since
            ),
            Some("no resolver has asked yet \u{2014} check delegation".into()),
        ),
    }
}

/// Cloudflare mode. Analytics prove only that resolvers ASK: Cloudflare answers NOERROR even
/// for names that do not exist, so the response code says nothing; step 2 proves the answers.
/// `now` is a unix time, passed in so the window is the caller's.
async fn cloudflare_asking(
    net: &NetworkConfig,
    preset: &CoinPreset,
    names: &Names,
    now: u64,
) -> Check {
    const NAME: &str = "real resolvers are asking";
    let skip = |detail: String, fix: Option<String>| check(4, NAME, Status::Skip, detail, fix);
    let Some(cf) = &net.cloudflare else {
        return skip("no [network.cloudflare] table".into(), None);
    };
    let token_env = std::env::var(config::TOKEN_ENV).ok();
    let client = match config::resolve_token(cf, token_env.as_deref())
        .and_then(|t| Client::new(&cf.api_base, &cf.zone_id, t))
    {
        Ok(c) => c,
        Err(e) => return skip(format!("{e:#}"), None),
    };
    let Ok(apex) = dns_name(&names.seed) else {
        return skip("bad seed name".into(), None);
    };
    let published: Vec<String> = published_names(&apex, &preset.filters)
        .iter()
        .map(|(n, _)| normalize_name(&n.to_ascii()))
        .collect();
    let (rows, warning) = match client
        .dns_analytics(&published, now.saturating_sub(FRESH_SECS), now)
        .await
    {
        Ok(a) => (a.rows, a.warning),
        Err(e @ ApiError::AnalyticsUnavailable(_)) => {
            return skip(
                e.to_string(),
                Some(
                    "give the token Zone \u{2192} Analytics \u{2192} Read to enable this check"
                        .into(),
                ),
            );
        }
        Err(e) => return skip(format!("could not read Cloudflare analytics: {e}"), None),
    };
    // Every name the check covers, so a silent one shows as `~0 queries`.
    let mut seen = Vec::new();
    let mut any = false;
    for name in names.all() {
        let mine = rows.iter().filter(|r| r.name == name && r.count > 0);
        let total: u64 = mine.clone().map(|r| r.count).sum();
        match mine.map(|r| r.hour.as_str()).max() {
            Some(last) => {
                any = true;
                seen.push(format!(
                    "{name} ~{total} queries, last {} UTC",
                    hour_of_day(last)
                ));
            }
            None => seen.push(format!("{name} ~0 queries")),
        }
    }
    if !any {
        return check(
            4,
            NAME,
            Status::Fail,
            format!(
                "no queries for {} in the last 24 h (Cloudflare DNS analytics, sampled)",
                names.all().join(" or ")
            ),
            Some(
                "no resolver has asked for these names in 24 h \u{2014} check that the coin's \
                 chainparams list this seed name, and wait a while after first publishing"
                    .into(),
            ),
        );
    }
    let mut detail = format!("{} (sampled; includes your own lookups)", seen.join("; "));
    if let Some(w) = warning {
        let w = w.split_whitespace().collect::<Vec<_>>().join(" ");
        detail.push_str(&format!(" (Cloudflare also reported: {w})"));
    }
    check(4, NAME, Status::Pass, detail, None)
}

/// `10-25 15:00` from `2026-10-25T15:00:00Z`.
fn hour_of_day(rfc3339: &str) -> String {
    match (rfc3339.get(5..10), rfc3339.get(11..16)) {
        (Some(date), Some(time)) => format!("{date} {time}"),
        _ => rfc3339.to_string(),
    }
}

fn age(secs: u64) -> String {
    match secs {
        0..=119 => format!("{secs} s"),
        120..=7199 => format!("{} min", secs / 60),
        _ => format!("{} h", secs / 3600),
    }
}
