//! A table-driven recursive-resolver stand-in: `(name, type)` maps to answers or an rcode.
//! Names are matched case-insensitively; anything not in the table is NXDOMAIN. Replies carry
//! RD and RA like a recursive resolver's. Serves UDP and TCP on one loopback port.
#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use hickory_proto::op::{Message, MessageType, ResponseCode};
use hickory_proto::rr::{RData, Record, RecordType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;

#[derive(Clone, Debug)]
pub enum Reply {
    /// NOERROR with these records; an empty list is NODATA.
    Answers(Vec<RData>),
    Rcode(ResponseCode),
}

#[derive(Clone, Debug)]
pub struct Seen {
    pub name: String,
    pub kind: RecordType,
    pub rd: bool,
    pub tcp: bool,
}

type Table = HashMap<(String, RecordType), Reply>;

pub struct FakeResolver {
    pub addr: SocketAddr,
    table: Arc<Mutex<Table>>,
    seen: Arc<Mutex<Vec<Seen>>>,
    noisy: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
}

fn key(name: &str, kind: RecordType) -> (String, RecordType) {
    (name.trim_end_matches('.').to_ascii_lowercase(), kind)
}

impl FakeResolver {
    pub async fn start() -> Self {
        Self::start_with(false).await
    }

    /// With `truncate_udp`, every UDP reply is an empty answer with TC set, so a client has to
    /// ask again over TCP.
    pub async fn start_with(truncate_udp: bool) -> Self {
        let (tcp, udp) = loop {
            let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = tcp.local_addr().unwrap().port();
            if let Ok(udp) = UdpSocket::bind(("127.0.0.1", port)).await {
                break (tcp, udp);
            }
        };
        let addr = udp.local_addr().unwrap();
        let table: Arc<Mutex<Table>> = Arc::default();
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();

        let noisy: Arc<AtomicBool> = Arc::default();
        let (t, s, nz) = (table.clone(), seen.clone(), noisy.clone());
        let udp_task = tokio::spawn(async move {
            let mut buf = [0u8; 1500];
            while let Ok((n, from)) = udp.recv_from(&mut buf).await {
                if let Some(out) = respond(&t, &s, &buf[..n], false, truncate_udp) {
                    if nz.load(Ordering::SeqCst) {
                        // Garbage, then a forged reply with the wrong id, before the real one.
                        let _ = udp.send_to(&[0xde, 0xad, 0xbe], from).await;
                        let mut forged = Message::from_vec(&out).unwrap();
                        forged.metadata.id = forged.metadata.id.wrapping_add(1);
                        forged.answers.clear();
                        forged.add_answer(Record::from_rdata(
                            forged.queries[0].name().clone(),
                            60,
                            RData::A(hickory_proto::rr::rdata::A("203.0.113.99".parse().unwrap())),
                        ));
                        let _ = udp.send_to(&forged.to_vec().unwrap(), from).await;
                    }
                    let _ = udp.send_to(&out, from).await;
                }
            }
        });
        let (t, s) = (table.clone(), seen.clone());
        let tcp_task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = tcp.accept().await {
                let (t, s) = (t.clone(), s.clone());
                tokio::spawn(async move {
                    let mut len = [0u8; 2];
                    if stream.read_exact(&mut len).await.is_err() {
                        return;
                    }
                    let mut body = vec![0u8; usize::from(u16::from_be_bytes(len))];
                    if stream.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    if let Some(out) = respond(&t, &s, &body, true, false) {
                        let _ = stream.write_all(&(out.len() as u16).to_be_bytes()).await;
                        let _ = stream.write_all(&out).await;
                    }
                });
            }
        });
        Self {
            addr,
            table,
            seen,
            noisy,
            tasks: vec![udp_task, tcp_task],
        }
    }

    pub fn set(&self, name: &str, kind: RecordType, reply: Reply) -> &Self {
        self.table.lock().unwrap().insert(key(name, kind), reply);
        self
    }

    pub fn answers(&self, name: &str, kind: RecordType, data: Vec<RData>) -> &Self {
        self.set(name, kind, Reply::Answers(data))
    }

    /// Precede every UDP reply with a malformed datagram and a wrong-id forgery.
    pub fn be_noisy(&self) {
        self.noisy.store(true, Ordering::SeqCst);
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

impl Drop for FakeResolver {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

fn respond(
    table: &Mutex<Table>,
    seen: &Mutex<Vec<Seen>>,
    wire: &[u8],
    tcp: bool,
    truncate: bool,
) -> Option<Vec<u8>> {
    let req = Message::from_vec(wire).ok()?;
    let q = req.queries.first()?.clone();
    let name = q.name().to_ascii();
    seen.lock().unwrap().push(Seen {
        name: name.trim_end_matches('.').to_ascii_lowercase(),
        kind: q.query_type(),
        rd: req.metadata.recursion_desired,
        tcp,
    });
    let mut msg = Message::new(req.metadata.id, MessageType::Response, req.metadata.op_code);
    msg.add_query(q.clone());
    msg.metadata.recursion_desired = req.metadata.recursion_desired;
    msg.metadata.recursion_available = true;
    if truncate {
        msg.metadata.truncation = true;
        return msg.to_vec().ok();
    }
    let reply = table
        .lock()
        .unwrap()
        .get(&key(&name, q.query_type()))
        .cloned()
        .unwrap_or(Reply::Rcode(ResponseCode::NXDomain));
    match reply {
        Reply::Answers(data) => {
            for d in data {
                msg.add_answer(Record::from_rdata(q.name().clone(), 60, d));
            }
        }
        Reply::Rcode(rc) => msg.metadata.response_code = rc,
    }
    msg.to_vec().ok()
}
