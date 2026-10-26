mod support;

use std::future::Future;
use std::net::IpAddr;
use std::time::Duration;

use seedcrystal::cloudflare::api::{ApiError, Batch, Client, NewRecord, OWNER_COMMENT, RecordKind};
use support::fake_cloudflare::{FakeCloudflare, FakeRecord};

const ZONE: &str = "zone1";
const TOKEN: &str = "s3cret-token-value";

async fn within<T>(f: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("test timed out")
}

async fn setup() -> (FakeCloudflare, Client) {
    let fake = FakeCloudflare::start(ZONE, TOKEN).await;
    let client = Client::new(&fake.base_url(), ZONE, TOKEN.to_string()).unwrap();
    (fake, client)
}

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

#[tokio::test]
async fn lists_records_across_pages() {
    let (fake, client) = setup().await;
    for i in 0..1200u32 {
        let content = format!("10.{}.{}.{}", i >> 16, (i >> 8) & 255, i & 255);
        fake.seed(FakeRecord::new(
            "Seed.Example.com",
            "A",
            &content,
            Some(OWNER_COMMENT),
        ));
    }
    fake.seed(FakeRecord::new(
        "seed.example.com",
        "AAAA",
        "2001:db8::1",
        None,
    ));
    fake.seed(FakeRecord::new("other.example.com", "A", "192.0.2.1", None));
    fake.seed(FakeRecord::new("seed.example.com", "TXT", "hello", None));

    let records = within(client.records_for("SEED.example.com."))
        .await
        .unwrap();
    assert_eq!(records.len(), 1201);
    assert_eq!(
        records.iter().filter(|r| r.kind == RecordKind::A).count(),
        1200
    );
    let aaaa = records.iter().find(|r| r.kind == RecordKind::Aaaa).unwrap();
    assert_eq!(aaaa.content, ip("2001:db8::1"));
    assert_eq!(aaaa.comment, None);
    assert!(records.iter().all(|r| r.name == "seed.example.com"));
    let page_calls = fake
        .requests()
        .iter()
        .filter(|r| r.method == "GET" && r.query.contains("type=A&"))
        .count();
    assert_eq!(page_calls, 3);
}

#[tokio::test]
async fn apply_posts_with_owner_comment_and_no_proxy() {
    let (fake, client) = setup().await;
    let batch = Batch {
        deletes: vec![],
        posts: vec![
            NewRecord {
                name: "Seed.Example.com".into(),
                kind: RecordKind::A,
                content: ip("192.0.2.7"),
                ttl: 300,
            },
            NewRecord {
                name: "seed.example.com".into(),
                kind: RecordKind::Aaaa,
                content: ip("2001:db8::7"),
                ttl: 120,
            },
        ],
    };
    within(client.apply(&batch)).await.unwrap();
    let records = fake.records();
    assert_eq!(records.len(), 2);
    for r in &records {
        assert_eq!(r.comment.as_deref(), Some("managed by seedcrystal"));
        assert!(!r.proxied);
        assert_eq!(r.name, "seed.example.com");
    }
    let a = records.iter().find(|r| r.kind == "A").unwrap();
    assert_eq!((a.content.as_str(), a.ttl), ("192.0.2.7", 300));
    let aaaa = records.iter().find(|r| r.kind == "AAAA").unwrap();
    assert_eq!((aaaa.content.as_str(), aaaa.ttl), ("2001:db8::7", 120));
    assert_eq!(fake.batch_ops(), vec![(0, 2)]);
}

#[tokio::test]
async fn apply_deletes_by_id() {
    let (fake, client) = setup().await;
    let gone = fake.seed(FakeRecord::new(
        "seed.example.com",
        "A",
        "192.0.2.1",
        Some(OWNER_COMMENT),
    ));
    let kept = fake.seed(FakeRecord::new("seed.example.com", "A", "192.0.2.2", None));
    let batch = Batch {
        deletes: vec![gone],
        posts: vec![],
    };
    within(client.apply(&batch)).await.unwrap();
    let left = fake.records();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].id, kept);

    let missing = Batch {
        deletes: vec!["nope".into()],
        posts: vec![],
    };
    let err = within(client.apply(&missing)).await.unwrap_err();
    assert!(matches!(err, ApiError::Rejected(_)), "{err:?}");
    assert_eq!(fake.records().len(), 1);
}

#[tokio::test]
async fn wrong_token_is_auth_error() {
    let fake = FakeCloudflare::start(ZONE, TOKEN).await;
    let client = Client::new(&fake.base_url(), ZONE, "wrong".to_string()).unwrap();
    let err = within(client.records_for("seed.example.com"))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::Auth(_)), "{err:?}");
    assert!(err.to_string().contains("Zone → DNS → Edit"), "{err}");
    let err = within(client.apply(&Batch::default())).await.unwrap_err();
    assert!(matches!(err, ApiError::Auth(_)), "{err:?}");

    fake.fail_always(Some(403));
    let good = Client::new(&fake.base_url(), ZONE, TOKEN.to_string()).unwrap();
    let err = within(good.records_for("seed.example.com"))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::Auth(_)), "{err:?}");
}

#[tokio::test]
async fn rate_limit_and_server_errors_are_typed() {
    let (fake, client) = setup().await;
    fake.fail_next(429);
    let err = within(client.records_for("seed.example.com"))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::RateLimited), "{err:?}");
    fake.fail_next(503);
    let err = within(client.apply(&Batch::default())).await.unwrap_err();
    assert!(matches!(err, ApiError::Server(503)), "{err:?}");
    fake.fail_always(Some(500));
    let err = within(client.records_for("seed.example.com"))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::Server(500)), "{err:?}");
    fake.fail_always(None);
    within(client.records_for("seed.example.com"))
        .await
        .unwrap();
}

#[tokio::test]
async fn oversized_batch_is_rejected_by_the_api() {
    let (fake, client) = setup().await;
    let posts = (0..201)
        .map(|i| NewRecord {
            name: "seed.example.com".into(),
            kind: RecordKind::A,
            content: ip(&format!("192.0.2.{}", i % 250)),
            ttl: 300,
        })
        .collect();
    let err = within(client.apply(&Batch {
        deletes: vec![],
        posts,
    }))
    .await
    .unwrap_err();
    assert!(matches!(err, ApiError::Rejected(_)), "{err:?}");
    assert!(fake.records().is_empty());
}

#[tokio::test]
async fn token_never_in_error_text() {
    let (fake, client) = setup().await;
    let wrong = Client::new(&fake.base_url(), ZONE, TOKEN.to_string()).unwrap();
    let mut errors = Vec::new();
    for status in [401u16, 403, 429, 500, 503, 400] {
        fake.fail_next(status);
        errors.push(wrong.records_for("seed.example.com").await.unwrap_err());
    }
    errors.push(
        wrong
            .apply(&Batch {
                deletes: vec!["nope".into()],
                posts: vec![],
            })
            .await
            .unwrap_err(),
    );
    // Nothing listens here: a transport error.
    let dead = Client::new("http://127.0.0.1:1/client/v4", ZONE, TOKEN.to_string()).unwrap();
    errors.push(
        within(dead.records_for("seed.example.com"))
            .await
            .unwrap_err(),
    );
    assert!(matches!(errors.last(), Some(ApiError::Transport(_))));
    for e in &errors {
        assert!(!e.to_string().contains(TOKEN), "{e}");
        assert!(!format!("{e:?}").contains(TOKEN), "{e:?}");
    }
    assert!(!format!("{client:?}").contains(TOKEN));
}

#[tokio::test]
async fn malformed_responses_are_errors_not_panics() {
    let (fake, client) = setup().await;
    // A non-Cloudflare server answering 200 with garbage.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf).await;
                let _ = s
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\nconnection: close\r\n\r\nnot json")
                    .await;
            });
        }
    });
    let bad = Client::new(&format!("http://{addr}/client/v4"), ZONE, TOKEN.to_string()).unwrap();
    let err = within(bad.records_for("seed.example.com"))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::Transport(_)), "{err:?}");
    let err = within(bad.apply(&Batch::default())).await.unwrap_err();
    assert!(matches!(err, ApiError::Transport(_)), "{err:?}");
    let _ = (fake, client);
}

#[tokio::test]
async fn http_400_with_credential_codes_is_auth() {
    let (fake, client) = setup().await;
    for code in [6003, 6111, 9109] {
        fake.fail_next_coded(400, code);
        let err = within(client.records_for("seed.example.com"))
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::Auth(_)), "{code}: {err:?}");
    }
    fake.fail_next_coded(400, 1004);
    let err = within(client.records_for("seed.example.com"))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::Rejected(_)), "{err:?}");
}

#[tokio::test]
async fn a_server_that_ignores_page_hits_the_cap_instead_of_truncating() {
    let (fake, client) = setup().await;
    fake.seed(FakeRecord::new(
        "seed.example.com",
        "A",
        "203.0.113.1",
        None,
    ));
    fake.ignore_page(Some(5000));
    let err = within(client.records_for("seed.example.com"))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ApiError::Transport(m) if m == "too many pages"),
        "{err:?}"
    );
}

#[tokio::test]
async fn an_empty_page_ends_the_listing() {
    let (fake, client) = setup().await;
    // total_pages claims more, but there is nothing at page 2.
    fake.seed(FakeRecord::new(
        "seed.example.com",
        "A",
        "203.0.113.1",
        None,
    ));
    fake.ignore_page(Some(1));
    let recs = within(client.records_for("seed.example.com"))
        .await
        .unwrap();
    assert_eq!(recs.len(), 1);
}

#[tokio::test]
async fn redirects_are_errors_and_not_followed() {
    let (fake, client) = setup().await;
    fake.redirect_always(true);
    let err = within(client.records_for("seed.example.com"))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::Transport(_)), "{err:?}");
    assert!(err.to_string().contains("redirect"), "{err}");
    assert_eq!(fake.requests().len(), 1);
}

#[tokio::test]
async fn records_report_ttl_and_proxied() {
    let (fake, client) = setup().await;
    let mut r = FakeRecord::new("seed.example.com", "A", "203.0.113.1", None);
    r.proxied = true;
    r.ttl = 1;
    fake.seed(r);
    let recs = within(client.records_for("seed.example.com"))
        .await
        .unwrap();
    assert!(recs[0].proxied);
    assert_eq!(recs[0].ttl, 1);
}

#[tokio::test]
async fn a_wrong_zone_id_is_a_zone_error_without_the_token() {
    let (fake, good) = setup().await;
    let client = Client::new(
        &fake.base_url(),
        "no-such-zone",
        "s3cret-token-value".into(),
    )
    .unwrap();
    let err = within(client.records_for("seed.example.com"))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::Zone(_)), "{err:?}");
    let text = err.to_string();
    assert!(
        text.contains("copy it from the zone's Overview page"),
        "{text}"
    );
    assert!(!text.contains("s3cret"), "{text}");
    for code in [7003, 7000] {
        fake.fail_next_coded(404, code);
        let err = within(good.records_for("seed.example.com"))
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::Zone(_)), "{code}: {err:?}");
    }
}

#[tokio::test]
async fn analytics_rows_are_parsed_and_the_query_is_sent() {
    let (fake, client) = setup().await;
    fake.set_analytics_rows(&[
        ("x1.Dnsseed.Example.com", "2026-10-25T15:00:00Z", 20),
        ("dnsseed.example.com", "2026-10-25T11:00:00Z", 30),
    ]);
    let names = vec![
        "dnsseed.example.com".to_string(),
        "x1.dnsseed.example.com".into(),
    ];
    let rows = within(client.dns_analytics(&names, 1_782_314_390, 1_782_400_790))
        .await
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "x1.dnsseed.example.com");
    assert_eq!(rows[0].hour, "2026-10-25T15:00:00Z");
    assert_eq!(rows[0].count, 20);
    assert_eq!(rows[1].count, 30);
    let sent = fake.requests().pop().unwrap().graphql.unwrap();
    assert_eq!(sent["variables"]["z"], ZONE);
    assert_eq!(sent["variables"]["since"], "2026-06-24T15:19:50Z");
    assert_eq!(sent["variables"]["until"], "2026-06-25T15:19:50Z");
    assert_eq!(sent["variables"]["names"][1], "x1.dnsseed.example.com");
}

#[tokio::test]
async fn analytics_errors_are_unavailable_with_cloudflares_message() {
    let (fake, client) = setup().await;
    fake.set_analytics_error("unknown field \"max\"");
    let err = within(client.dns_analytics(&["a.example.com".into()], 0, 1))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::AnalyticsUnavailable(_)), "{err:?}");
    let text = err.to_string();
    assert!(text.contains("unknown field \"max\""), "{text}");
    assert!(text.contains("Analytics"), "{text}");
    assert!(!text.contains(TOKEN));
}

#[tokio::test]
async fn empty_zones_means_the_token_cannot_see_the_zone() {
    let (fake, client) = setup().await;
    fake.set_analytics_no_zones();
    let err = within(client.dns_analytics(&["a.example.com".into()], 0, 1))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::AnalyticsUnavailable(_)), "{err:?}");
    let text = err.to_string();
    assert!(
        text.contains(
            "this token cannot see the zone \u{2014} give it Zone \u{2192} Analytics \u{2192} \
             Read on this zone"
        ),
        "{text}"
    );
    assert!(!text.contains("malformed"), "{text}");
}

#[tokio::test]
async fn partial_errors_keep_the_rows_and_surface_the_first_message() {
    let (fake, client) = setup().await;
    fake.set_analytics_rows(&[("a.example.com", "2026-10-25T15:00:00Z", 20)]);
    fake.set_analytics_partial_error("first problem");
    let got = within(client.dns_analytics(&["a.example.com".into()], 0, 1))
        .await
        .unwrap();
    assert_eq!(got.rows.len(), 1);
    assert_eq!(got.warning.as_deref(), Some("first problem"));
}

#[tokio::test]
async fn partial_errors_without_rows_are_unavailable() {
    let (fake, client) = setup().await;
    fake.set_analytics_partial_error("only a problem");
    let err = within(client.dns_analytics(&["a.example.com".into()], 0, 1))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::AnalyticsUnavailable(_)), "{err:?}");
    assert!(err.to_string().contains("only a problem"));
}

#[tokio::test]
async fn analytics_403_names_the_permission_and_never_the_token() {
    let (fake, client) = setup().await;
    fake.fail_next(403);
    let err = within(client.dns_analytics(&["a.example.com".into()], 0, 1))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::AnalyticsUnavailable(_)), "{err:?}");
    let text = format!("{err} {err:?}");
    assert!(
        text.contains("Zone \u{2192} Analytics \u{2192} Read"),
        "{text}"
    );
    assert!(!text.contains(TOKEN));
}

#[tokio::test]
async fn analytics_wrong_token_is_unavailable_and_redacted() {
    let (fake, _) = setup().await;
    let client = Client::new(&fake.base_url(), ZONE, "other-secret".to_string()).unwrap();
    let err = within(client.dns_analytics(&["a.example.com".into()], 0, 1))
        .await
        .unwrap_err();
    assert!(matches!(err, ApiError::AnalyticsUnavailable(_)), "{err:?}");
    assert!(!format!("{err} {err:?} {client:?}").contains("other-secret"));
}
