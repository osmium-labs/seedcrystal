//! A small client for the Cloudflare DNS records API: list a name's A and AAAA records, and apply
//! deletes and creates in one batch call.

use std::fmt;
use std::net::IpAddr;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};

/// The comment on every record the seeder creates; it is the proof of ownership.
pub const OWNER_COMMENT: &str = "managed by seedcrystal";

pub const DEFAULT_API_BASE: &str = "https://api.cloudflare.com/client/v4";

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const PER_PAGE: usize = 500;
/// Upper bound on list pages for one name and type, so a misbehaving server cannot loop us.
const MAX_PAGES: usize = 1000;

/// Cloudflare error codes that mean the token or its permissions are wrong, even on HTTP 400.
const AUTH_CODES: [i64; 3] = [6003, 6111, 9109];
/// Cloudflare error codes for a zone id that does not route: 7003 ("Could not route to
/// /zones/..., perhaps your object identifier is invalid?") and 7000 ("No route for that URI").
const ZONE_CODES: [i64; 2] = [7003, 7000];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordKind {
    A,
    Aaaa,
}

impl RecordKind {
    fn as_str(self) -> &'static str {
        match self {
            RecordKind::A => "A",
            RecordKind::Aaaa => "AAAA",
        }
    }
}

/// Sampled query count for one name in one hour (counts are multiples of 10).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryCount {
    /// Lowercase, no trailing dot.
    pub name: String,
    /// Cloudflare's `datetimeHour`, e.g. `2026-10-25T15:00:00Z`.
    pub hour: String,
    pub count: u64,
}

const ANALYTICS_QUERY: &str = "query($z: string!, $since: Time!, $until: Time!, $names: [string!]) \
{ viewer { zones(filter: {zoneTag: $z}) { dnsAnalyticsAdaptiveGroups(limit: 200, \
filter: {datetime_geq: $since, datetime_leq: $until, queryName_in: $names}, \
orderBy: [datetimeHour_DESC]) { count dimensions { queryName responseCode datetimeHour } } } } }";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub id: String,
    /// Lowercase, no trailing dot.
    pub name: String,
    pub kind: RecordKind,
    pub content: IpAddr,
    pub ttl: u32,
    pub proxied: bool,
    pub comment: Option<String>,
}

/// A record to create. It is posted with [`OWNER_COMMENT`] and `proxied: false`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewRecord {
    pub name: String,
    pub kind: RecordKind,
    pub content: IpAddr,
    pub ttl: u32,
}

/// Record ids to delete and records to create; Cloudflare runs deletes first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Batch {
    pub deletes: Vec<String>,
    pub posts: Vec<NewRecord>,
}

/// Query counts plus the first error Cloudflare reported alongside them, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Analytics {
    pub rows: Vec<QueryCount>,
    pub warning: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// HTTP 401 or 403.
    #[error(
        "Cloudflare refused the API token ({0}); it needs the permission Zone → DNS → Edit on this zone"
    )]
    Auth(String),
    /// HTTP 400 or 404 with a routing error code.
    #[error(
        "zone_id was not found or is malformed — copy it from the zone's Overview page (API section)"
    )]
    Zone(String),
    /// The analytics query failed: a GraphQL `errors` entry, or HTTP 401/403. Carries
    /// Cloudflare's message (never the token).
    #[error(
        "Cloudflare analytics unavailable: {0}; the token needs Zone \u{2192} Analytics \u{2192} Read"
    )]
    AnalyticsUnavailable(String),
    #[error("Cloudflare rate limit hit (HTTP 429)")]
    RateLimited,
    #[error("Cloudflare server error (HTTP {0})")]
    Server(u16),
    /// The API answered `success: false`.
    #[error("Cloudflare rejected the request: {0}")]
    Rejected(String),
    #[error("could not talk to Cloudflare: {0}")]
    Transport(String),
}

pub struct Client {
    http: reqwest::Client,
    api_base: String,
    zone_id: String,
    token: String,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("api_base", &self.api_base)
            .field("zone_id", &self.zone_id)
            .field("token", &"<redacted>")
            .finish()
    }
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    errors: Vec<ApiMessage>,
    #[serde(default)]
    result: Value,
    result_info: Option<ResultInfo>,
}

#[derive(Deserialize)]
struct ApiMessage {
    code: Option<i64>,
    message: Option<String>,
}

#[derive(Deserialize)]
struct ResultInfo {
    total_pages: Option<usize>,
}

#[derive(Deserialize)]
struct WireRecord {
    id: String,
    name: String,
    #[serde(rename = "type")]
    kind: String,
    content: String,
    #[serde(default)]
    ttl: u32,
    #[serde(default)]
    proxied: bool,
    comment: Option<String>,
}

/// Lowercase, without a trailing dot.
pub fn normalize_name(name: &str) -> String {
    name.trim_end_matches('.').to_ascii_lowercase()
}

impl Client {
    pub fn new(api_base: &str, zone_id: &str, token: String) -> anyhow::Result<Client> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("seedcrystal/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| anyhow::anyhow!("building the HTTP client: {}", e.without_url()))?;
        Ok(Client {
            http,
            api_base: api_base.trim_end_matches('/').to_string(),
            zone_id: zone_id.to_string(),
            token,
        })
    }

    fn records_url(&self) -> String {
        format!("{}/zones/{}/dns_records", self.api_base, self.zone_id)
    }

    /// Every A and AAAA record whose name is exactly `name`, whoever owns it.
    pub async fn records_for(&self, name: &str) -> Result<Vec<Record>, ApiError> {
        let name = normalize_name(name);
        let mut out = Vec::new();
        for kind in [RecordKind::A, RecordKind::Aaaa] {
            let mut page = 1usize;
            loop {
                let request = self.http.get(self.records_url()).query(&[
                    ("name.exact", name.as_str()),
                    ("type", kind.as_str()),
                    ("per_page", &PER_PAGE.to_string()),
                    ("page", &page.to_string()),
                ]);
                let env = self.send(request).await?;
                let total = env
                    .result_info
                    .as_ref()
                    .and_then(|i| i.total_pages)
                    .unwrap_or(1);
                let wire: Vec<WireRecord> = serde_json::from_value(env.result)
                    .map_err(|_| ApiError::Transport("malformed record list".into()))?;
                if wire.is_empty() {
                    break;
                }
                for w in wire {
                    // A record we cannot read is not one we could own; skip it.
                    let Ok(content) = w.content.parse::<IpAddr>() else {
                        continue;
                    };
                    let w_kind = match (w.kind.as_str(), content) {
                        ("A", IpAddr::V4(_)) => RecordKind::A,
                        ("AAAA", IpAddr::V6(_)) => RecordKind::Aaaa,
                        _ => continue,
                    };
                    out.push(Record {
                        id: w.id,
                        name: normalize_name(&w.name),
                        kind: w_kind,
                        content,
                        ttl: w.ttl,
                        proxied: w.proxied,
                        comment: w.comment,
                    });
                }
                // Count our own pages: a server that ignores `page` must not loop us or
                // hand back a silently truncated list.
                if page >= total {
                    break;
                }
                if page >= MAX_PAGES {
                    return Err(ApiError::Transport("too many pages".into()));
                }
                page += 1;
            }
        }
        Ok(out)
    }

    /// Runs the batch as one call. The caller keeps it to at most 200 operations.
    pub async fn apply(&self, batch: &Batch) -> Result<(), ApiError> {
        let body = json!({
            "deletes": batch.deletes.iter().map(|id| json!({"id": id})).collect::<Vec<_>>(),
            "posts": batch.posts.iter().map(|r| json!({
                "name": normalize_name(&r.name),
                "type": r.kind.as_str(),
                "content": r.content.to_string(),
                "ttl": r.ttl,
                "proxied": false,
                "comment": OWNER_COMMENT,
            })).collect::<Vec<_>>(),
        });
        let request = self
            .http
            .post(format!("{}/batch", self.records_url()))
            .json(&body);
        self.send(request).await.map(|_| ())
    }

    /// Sampled DNS query counts per name and hour between two unix times (seconds, UTC).
    /// HTTP 401/403, and any GraphQL `errors` entry with no usable rows, is
    /// [`ApiError::AnalyticsUnavailable`]; with usable rows the first error rides along as
    /// [`Analytics::warning`].
    pub async fn dns_analytics(
        &self,
        names: &[String],
        since_unix: u64,
        until_unix: u64,
    ) -> Result<Analytics, ApiError> {
        let body = json!({
            "query": ANALYTICS_QUERY,
            "variables": {
                "z": self.zone_id,
                "since": rfc3339_utc(since_unix),
                "until": rfc3339_utc(until_unix),
                "names": names.iter().map(|n| normalize_name(n)).collect::<Vec<_>>(),
            },
        });
        let response = self
            .http
            .post(format!("{}/graphql", self.api_base))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(transport)?;
        let code = response.status().as_u16();
        match code {
            401 | 403 => return Err(ApiError::AnalyticsUnavailable(format!("HTTP {code}"))),
            429 => return Err(ApiError::RateLimited),
            500..=599 => return Err(ApiError::Server(code)),
            300..=399 => {
                return Err(ApiError::Transport(format!(
                    "unexpected redirect (HTTP {code}); refusing to follow it"
                )));
            }
            _ => {}
        }
        let bytes = response.bytes().await.map_err(transport)?;
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| ApiError::Transport(format!("malformed response (HTTP {code})")))?;
        let errors: Vec<&str> = value
            .get("errors")
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .map(|e| {
                        e.get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown error")
                    })
                    .collect()
            })
            .unwrap_or_default();
        let unavailable = || ApiError::AnalyticsUnavailable(errors.join("; "));
        if code != 200 {
            return Err(if errors.is_empty() {
                ApiError::Rejected(format!("HTTP {code}"))
            } else {
                unavailable()
            });
        }
        let malformed = || ApiError::Transport("malformed analytics response".into());
        let parsed = parse_analytics_rows(&value, malformed);
        match (errors.is_empty(), parsed) {
            (true, parsed) => parsed.map(|rows| Analytics {
                rows,
                warning: None,
            }),
            // Partial data: usable rows win, and the first message is kept for the caller.
            (false, Ok(rows)) if !rows.is_empty() => Ok(Analytics {
                rows,
                warning: Some(errors[0].to_string()),
            }),
            (false, _) => Err(unavailable()),
        }
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Result<Envelope, ApiError> {
        let response = request
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(transport)?;
        let status = response.status();
        let code = status.as_u16();
        match code {
            401 | 403 => return Err(ApiError::Auth(format!("HTTP {code}"))),
            429 => return Err(ApiError::RateLimited),
            500..=599 => return Err(ApiError::Server(code)),
            300..=399 => {
                return Err(ApiError::Transport(format!(
                    "unexpected redirect (HTTP {code}); refusing to follow it"
                )));
            }
            _ => {}
        }
        let bytes = response.bytes().await.map_err(transport)?;
        let env: Envelope = serde_json::from_slice(&bytes)
            .map_err(|_| ApiError::Transport(format!("malformed response (HTTP {code})")))?;
        // Cloudflare answers some bad credentials with a 400 and one of these codes.
        if code == 400
            && env
                .errors
                .iter()
                .any(|e| AUTH_CODES.contains(&e.code.unwrap_or(0)))
        {
            return Err(ApiError::Auth(format!("HTTP {code}")));
        }
        if matches!(code, 400 | 404)
            && env
                .errors
                .iter()
                .any(|e| ZONE_CODES.contains(&e.code.unwrap_or(0)))
        {
            return Err(ApiError::Zone(format!("HTTP {code}")));
        }
        if !status.is_success() || !env.success {
            return Err(ApiError::Rejected(describe(&env.errors, code)));
        }
        Ok(env)
    }
}

/// The per-hour rows of the first zone in a GraphQL body. An empty `zones` list means the
/// token cannot see the zone.
fn parse_analytics_rows(
    value: &Value,
    malformed: impl Fn() -> ApiError,
) -> Result<Vec<QueryCount>, ApiError> {
    let zones = value
        .pointer("/data/viewer/zones")
        .and_then(Value::as_array)
        .ok_or_else(&malformed)?;
    let Some(zone) = zones.first() else {
        return Err(ApiError::AnalyticsUnavailable(
            "this token cannot see the zone \u{2014} give it Zone \u{2192} Analytics \u{2192} \
             Read on this zone"
                .into(),
        ));
    };
    let rows = zone
        .get("dnsAnalyticsAdaptiveGroups")
        .and_then(Value::as_array)
        .ok_or_else(&malformed)?;
    rows.iter()
        .map(|row| {
            let dim = |k: &str| {
                row.pointer(&format!("/dimensions/{k}"))
                    .and_then(Value::as_str)
            };
            Ok(QueryCount {
                name: normalize_name(dim("queryName").ok_or_else(&malformed)?),
                hour: dim("datetimeHour").ok_or_else(&malformed)?.to_string(),
                count: row
                    .get("count")
                    .and_then(Value::as_u64)
                    .ok_or_else(&malformed)?,
            })
        })
        .collect()
}

/// `2026-10-24T15:19:50Z` for a unix time (proleptic Gregorian, days-from-civil).
fn rfc3339_utc(unix: u64) -> String {
    let (days, rem) = ((unix / 86_400) as i64, unix % 86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn transport(e: reqwest::Error) -> ApiError {
    ApiError::Transport(e.without_url().to_string())
}

fn describe(errors: &[ApiMessage], status: u16) -> String {
    let parts: Vec<String> = errors
        .iter()
        .map(|e| match (e.code, e.message.as_deref()) {
            (Some(c), Some(m)) => format!("{m} (code {c})"),
            (None, Some(m)) => m.to_string(),
            (Some(c), None) => format!("code {c}"),
            (None, None) => "unknown error".to_string(),
        })
        .collect();
    if parts.is_empty() {
        format!("HTTP {status}")
    } else {
        parts.join("; ")
    }
}

#[cfg(test)]
mod tests {
    use super::rfc3339_utc;

    #[test]
    fn formats_calendar_edges() {
        // 2100 is not a leap year: Feb has 28 days, so 03-01 follows 02-28.
        assert_eq!(rfc3339_utc(4_107_542_400), "2100-03-01T00:00:00Z");
        assert_eq!(rfc3339_utc(4_107_542_400 - 86_400), "2100-02-28T00:00:00Z");
        assert_eq!(rfc3339_utc(1_798_761_599), "2026-12-31T23:59:59Z");
        assert_eq!(rfc3339_utc(1_798_761_599 + 1), "2027-01-01T00:00:00Z");
        assert_eq!(rfc3339_utc(1_767_582_245), "2026-01-05T03:04:05Z");
    }

    #[test]
    fn formats_unix_times() {
        assert_eq!(rfc3339_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_utc(1_782_314_390), "2026-06-24T15:19:50Z");
        assert_eq!(rfc3339_utc(951_782_400), "2000-02-29T00:00:00Z");
    }
}
