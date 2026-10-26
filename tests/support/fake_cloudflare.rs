//! An in-memory stand-in for the Cloudflare DNS records API: the list endpoint and the batch
//! endpoint, bearer-token checking, the 200-operation batch limit, a request log, and failure
//! switches.
#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

/// Batch operation limit of the Free plan.
pub const BATCH_LIMIT: usize = 200;
/// Largest `per_page` the list endpoint honours.
const MAX_PER_PAGE: usize = 5000;

#[derive(Clone, Debug, PartialEq)]
pub struct FakeRecord {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub content: String,
    pub ttl: u32,
    pub proxied: bool,
    pub comment: Option<String>,
}

impl FakeRecord {
    pub fn new(name: &str, kind: &str, content: &str, comment: Option<&str>) -> Self {
        Self {
            id: String::new(),
            name: name.to_ascii_lowercase(),
            kind: kind.to_string(),
            content: content.to_string(),
            ttl: 60,
            proxied: false,
            comment: comment.map(str::to_string),
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "id": self.id, "name": self.name, "type": self.kind, "content": self.content,
            "ttl": self.ttl, "proxied": self.proxied, "comment": self.comment,
            "created_on": "2026-01-01T00:00:00Z", "tags": [],
        })
    }
}

/// One request the fake saw.
#[derive(Clone, Debug)]
pub struct LoggedRequest {
    pub method: String,
    pub path: String,
    pub query: String,
    /// For a batch call: (deletes, posts) as sent, whether or not the call was accepted.
    pub ops: Option<(usize, usize)>,
    /// For a GraphQL call: the JSON body as sent.
    pub graphql: Option<Value>,
    pub at: std::time::Instant,
}

#[derive(Default)]
struct State_ {
    token: String,
    zone: String,
    records: Vec<FakeRecord>,
    next_id: u64,
    log: Vec<LoggedRequest>,
    fail_once: Vec<u16>,
    fail_batch_once: Vec<u16>,
    fail_always: Option<u16>,
    /// (status, Cloudflare error code) for the next request.
    fail_coded_once: Vec<(u16, i64)>,
    delay: std::time::Duration,
    ignore_page: bool,
    force_total_pages: Option<usize>,
    redirect_always: bool,
    zone_fault: bool,
    analytics_rows: Vec<Value>,
    analytics_errors: Option<String>,
    analytics_partial_error: Option<String>,
    analytics_no_zones: bool,
}

pub struct FakeCloudflare {
    addr: SocketAddr,
    state: Arc<Mutex<State_>>,
    server: JoinHandle<()>,
}

impl Drop for FakeCloudflare {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl FakeCloudflare {
    pub async fn start(zone: &str, token: &str) -> Self {
        let state = Arc::new(Mutex::new(State_ {
            token: token.to_string(),
            zone: zone.to_string(),
            ..State_::default()
        }));
        let app = Router::new().fallback(handle).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            addr,
            state,
            server,
        }
    }

    /// The `api_base` to hand to the client.
    pub fn base_url(&self) -> String {
        format!("http://{}/client/v4", self.addr)
    }

    /// Adds a record, assigning it an id. Returns the id.
    pub fn seed(&self, mut record: FakeRecord) -> String {
        let mut s = self.state.lock().unwrap();
        record.id = format!("rec{:06}", s.next_id);
        s.next_id += 1;
        let id = record.id.clone();
        s.records.push(record);
        id
    }

    /// Removes every record with this content, as if deleted in the dashboard.
    pub fn remove_content(&self, content: &str) {
        self.state
            .lock()
            .unwrap()
            .records
            .retain(|r| r.content != content);
    }

    pub fn records(&self) -> Vec<FakeRecord> {
        self.state.lock().unwrap().records.clone()
    }

    pub fn requests(&self) -> Vec<LoggedRequest> {
        self.state.lock().unwrap().log.clone()
    }

    /// Only the batch calls, as (deletes, posts).
    pub fn batch_ops(&self) -> Vec<(usize, usize)> {
        self.requests().iter().filter_map(|r| r.ops).collect()
    }

    /// The next request (of any kind) is answered with `status`; later ones are normal again.
    /// Calls stack: each call queues one more failure.
    pub fn fail_next(&self, status: u16) {
        self.state.lock().unwrap().fail_once.push(status);
    }

    /// The next batch call (only) is answered with `status`; list calls are unaffected.
    pub fn fail_next_batch(&self, status: u16) {
        self.state.lock().unwrap().fail_batch_once.push(status);
    }

    /// The next request is answered with `status` and one Cloudflare error with `code`.
    pub fn fail_next_coded(&self, status: u16, code: i64) {
        self.state
            .lock()
            .unwrap()
            .fail_coded_once
            .push((status, code));
    }

    /// While on, every request is answered with HTTP 400 and code 7003, as for a bad zone id.
    pub fn zone_fault(&self, on: bool) {
        self.state.lock().unwrap().zone_fault = on;
    }

    /// Rows the `/graphql` endpoint returns, as (name, hour, count); sent as given, not
    /// filtered by the query's names or window.
    pub fn set_analytics_rows(&self, rows: &[(&str, &str, u64)]) {
        self.state.lock().unwrap().analytics_rows = rows
            .iter()
            .map(|(n, h, c)| {
                json!({"count": c, "dimensions": {"queryName": n, "responseCode": "NOERROR",
                                                  "datetimeHour": h}})
            })
            .collect();
    }

    /// The `/graphql` endpoint returns its rows AND this error message in one 200 body.
    pub fn set_analytics_partial_error(&self, message: &str) {
        self.state.lock().unwrap().analytics_partial_error = Some(message.to_string());
    }

    /// The `/graphql` endpoint answers with `zones: []`, as for a token blind to the zone.
    pub fn set_analytics_no_zones(&self) {
        self.state.lock().unwrap().analytics_no_zones = true;
    }

    /// The `/graphql` endpoint answers HTTP 200 with `data: null` and this error message.
    pub fn set_analytics_error(&self, message: &str) {
        self.state.lock().unwrap().analytics_errors = Some(message.to_string());
    }

    /// Every request waits this long before it is looked at.
    pub fn set_delay(&self, delay: std::time::Duration) {
        self.state.lock().unwrap().delay = delay;
    }

    /// The list endpoint always returns page 1 and reports `total_pages` as given.
    pub fn ignore_page(&self, total_pages: Option<usize>) {
        let mut s = self.state.lock().unwrap();
        s.ignore_page = total_pages.is_some();
        s.force_total_pages = total_pages;
    }

    /// Every request is answered with a 302.
    pub fn redirect_always(&self, on: bool) {
        self.state.lock().unwrap().redirect_always = on;
    }

    /// Every request is answered with `status` until cleared with `None`.
    pub fn fail_always(&self, status: Option<u16>) {
        self.state.lock().unwrap().fail_always = status;
    }
}

fn envelope(status: StatusCode, body: Value) -> Response {
    (status, axum::Json(body)).into_response()
}

fn failure(status: u16) -> Response {
    let code = StatusCode::from_u16(status).unwrap();
    envelope(
        code,
        json!({"success": false, "errors": [{"code": status, "message": "injected failure"}],
               "messages": [], "result": null}),
    )
}

async fn handle(
    State(state): State<Arc<Mutex<State_>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let delay = state.lock().unwrap().delay;
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    let mut s = state.lock().unwrap();
    let path = uri.path().to_string();
    let query = uri.query().unwrap_or("").to_string();
    let mut entry = LoggedRequest {
        method: method.to_string(),
        path: path.clone(),
        query: query.clone(),
        ops: None,
        graphql: None,
        at: std::time::Instant::now(),
    };
    let list_path = format!("/client/v4/zones/{}/dns_records", s.zone);
    let batch_path = format!("{list_path}/batch");
    let parsed_body: Option<Value> = serde_json::from_slice(&body).ok();
    if path == batch_path
        && let Some(b) = &parsed_body
    {
        let count = |k: &str| b.get(k).and_then(Value::as_array).map_or(0, Vec::len);
        entry.ops = Some((count("deletes"), count("posts")));
    }
    if path == "/client/v4/graphql" {
        entry.graphql = parsed_body.clone();
    }
    s.log.push(entry);

    if s.redirect_always {
        return (
            StatusCode::FOUND,
            [("location", "http://127.0.0.1:1/elsewhere")],
        )
            .into_response();
    }
    if s.zone_fault {
        return envelope(
            StatusCode::BAD_REQUEST,
            json!({"success": false, "messages": [], "result": null,
                   "errors": [{"code": 7003, "message": "Could not route to /zones/x/dns_records, perhaps your object identifier is invalid?"}]}),
        );
    }
    if !s.fail_coded_once.is_empty() {
        let (status, code) = s.fail_coded_once.remove(0);
        return envelope(
            StatusCode::from_u16(status).unwrap(),
            json!({"success": false, "errors": [{"code": code, "message": "injected"}],
                   "messages": [], "result": null}),
        );
    }
    if !s.fail_once.is_empty() {
        let status = s.fail_once.remove(0);
        return failure(status);
    }
    if let Some(status) = s.fail_always {
        return failure(status);
    }
    if method == Method::POST && path == batch_path && !s.fail_batch_once.is_empty() {
        let status = s.fail_batch_once.remove(0);
        return failure(status);
    }
    let expected = format!("Bearer {}", s.token);
    let supplied = headers.get("authorization").and_then(|v| v.to_str().ok());
    if supplied != Some(expected.as_str()) {
        return failure(401);
    }
    if method == Method::POST && path == "/client/v4/graphql" {
        if let Some(message) = &s.analytics_errors {
            return envelope(
                StatusCode::OK,
                json!({"data": null, "errors": [{"message": message, "path": null}]}),
            );
        }
        if s.analytics_no_zones {
            return envelope(
                StatusCode::OK,
                json!({"data": {"viewer": {"zones": []}}, "errors": null}),
            );
        }
        let errors = match &s.analytics_partial_error {
            Some(m) => json!([{"message": m, "path": null}]),
            None => Value::Null,
        };
        return envelope(
            StatusCode::OK,
            json!({"data": {"viewer": {"zones": [{"dnsAnalyticsAdaptiveGroups": s.analytics_rows}]}},
                   "errors": errors}),
        );
    }
    if let Some(rest) = path.strip_prefix("/client/v4/zones/") {
        let zone = rest.split('/').next().unwrap_or("");
        if zone != s.zone {
            return envelope(
                StatusCode::BAD_REQUEST,
                json!({"success": false, "messages": [], "result": null,
                       "errors": [{"code": 7003, "message": format!(
                           "Could not route to /zones/{zone}/dns_records, perhaps your object identifier is invalid?")}]}),
            );
        }
    }

    if method == Method::GET && path == list_path {
        let q: HashMap<String, String> = url_query(&query);
        let name = q.get("name.exact").map(|n| n.to_ascii_lowercase());
        let kind = q.get("type");
        let per_page = q
            .get("per_page")
            .and_then(|p| p.parse::<usize>().ok())
            .unwrap_or(100)
            .clamp(1, MAX_PER_PAGE);
        let page = if s.ignore_page {
            1
        } else {
            q.get("page")
                .and_then(|p| p.parse::<usize>().ok())
                .unwrap_or(1)
                .max(1)
        };
        let matching: Vec<&FakeRecord> = s
            .records
            .iter()
            .filter(|r| name.as_ref().is_none_or(|n| &r.name == n))
            .filter(|r| kind.is_none_or(|k| &r.kind == k))
            .collect();
        let total = matching.len();
        let total_pages = s
            .force_total_pages
            .unwrap_or_else(|| total.div_ceil(per_page).max(1));
        let slice: Vec<Value> = matching
            .iter()
            .skip((page - 1) * per_page)
            .take(per_page)
            .map(|r| r.to_json())
            .collect();
        return envelope(
            StatusCode::OK,
            json!({"success": true, "errors": [], "messages": [], "result": slice,
                   "result_info": {"page": page, "per_page": per_page, "count": slice.len(),
                                   "total_count": total, "total_pages": total_pages}}),
        );
    }

    if method == Method::POST && path == batch_path {
        let Some(b) = parsed_body else {
            return reject("malformed JSON body");
        };
        let list = |k: &str| {
            b.get(k)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        let (deletes, patches, puts, posts) = (
            list("deletes"),
            list("patches"),
            list("puts"),
            list("posts"),
        );
        let total = deletes.len() + patches.len() + puts.len() + posts.len();
        if total > BATCH_LIMIT {
            return reject("too many operations in one batch");
        }
        // Transactional: validate everything before changing anything.
        let mut ids: Vec<&str> = Vec::new();
        for d in &deletes {
            let Some(id) = d.get("id").and_then(Value::as_str) else {
                return reject("delete without id");
            };
            if !s.records.iter().any(|r| r.id == id) || ids.contains(&id) {
                return reject("record does not exist");
            }
            ids.push(id);
        }
        let mut created = Vec::new();
        for p in &posts {
            let text = |k: &str| p.get(k).and_then(Value::as_str).map(str::to_string);
            let (Some(name), Some(kind), Some(content), Some(ttl)) = (
                text("name"),
                text("type"),
                text("content"),
                p.get("ttl").and_then(Value::as_u64),
            ) else {
                return reject("post missing a required field");
            };
            // Stricter than the real API, which defaults a missing `proxied` to false: the
            // seeder must send it explicitly.
            let Some(proxied) = p.get("proxied").and_then(Value::as_bool) else {
                return reject("post missing proxied");
            };
            created.push(FakeRecord {
                id: String::new(),
                name: name.to_ascii_lowercase(),
                kind,
                content,
                ttl: ttl as u32,
                proxied,
                comment: text("comment"),
            });
        }
        // Like Cloudflare: a post identical to an existing record (owned or not, and not
        // deleted by this same batch) rejects the whole batch.
        for (i, c) in created.iter().enumerate() {
            let same =
                |r: &FakeRecord| r.name == c.name && r.kind == c.kind && r.content == c.content;
            let clash = s
                .records
                .iter()
                .any(|r| same(r) && !ids.contains(&r.id.as_str()))
                || created[..i].iter().any(same);
            if clash {
                return envelope(
                    StatusCode::BAD_REQUEST,
                    json!({"success": false, "errors": [{"code": 81058,
                           "message": "An identical record already exists."}],
                           "messages": [], "result": null}),
                );
            }
        }
        s.records.retain(|r| !ids.contains(&r.id.as_str()));
        for mut r in created {
            r.id = format!("rec{:06}", s.next_id);
            s.next_id += 1;
            s.records.push(r);
        }
        return envelope(
            StatusCode::OK,
            json!({"success": true, "errors": [], "messages": [],
                   "result": {"deletes": [], "patches": [], "puts": [], "posts": []}}),
        );
    }

    envelope(
        StatusCode::NOT_FOUND,
        json!({"success": false, "errors": [{"code": 7003, "message": "no route"}],
               "messages": [], "result": null}),
    )
}

fn reject(message: &str) -> Response {
    envelope(
        StatusCode::BAD_REQUEST,
        json!({"success": false, "errors": [{"code": 1004, "message": message}],
               "messages": [], "result": null}),
    )
}

fn url_query(q: &str) -> HashMap<String, String> {
    q.split('&')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
