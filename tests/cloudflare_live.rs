//! Opt-in test against the real Cloudflare API. It creates three A records on one throwaway
//! name and deletes them again. Run it with:
//!
//! ```text
//! SEEDCRYSTAL_CF_LIVE_ZONE_ID=<zone id> \
//! SEEDCRYSTAL_CLOUDFLARE_TOKEN=<token with Zone > DNS > Edit> \
//! SEEDCRYSTAL_CF_LIVE_NAME=seedcrystal-test.<your zone> \
//! cargo test --test cloudflare_live -- --ignored
//! ```

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use hickory_proto::rr::Name;
use rand::SeedableRng;
use rand::rngs::StdRng;
use seedcrystal::cloudflare::api::{
    Batch, Client, DEFAULT_API_BASE, OWNER_COMMENT, RecordKind, normalize_name,
};
use seedcrystal::cloudflare::publish::{PublishConfig, reconcile_once};
use seedcrystal::crawler::Snapshot;
use seedcrystal::store::Servable;

const REQUIRED_LABEL: &str = "seedcrystal-test";

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

/// Deletes the records on `name` that carry the owner comment, and nothing else. On failure the
/// error names the record ids that may remain.
async fn cleanup(client: &Client, name: &str) -> Result<(), String> {
    let owned: Vec<String> = client
        .records_for(name)
        .await
        .map_err(|e| format!("listing records on {name}: {e}"))?
        .into_iter()
        .filter(|r| r.name == name && r.comment.as_deref() == Some(OWNER_COMMENT))
        .map(|r| r.id)
        .collect();
    if owned.is_empty() {
        return Ok(());
    }
    let batch = Batch {
        deletes: owned.clone(),
        posts: Vec::new(),
    };
    client
        .apply(&batch)
        .await
        .map_err(|e| format!("deleting {owned:?} on {name}: {e}"))
}

const DOC_ADDRS: [[u8; 4]; 3] = [[192, 0, 2, 1], [198, 51, 100, 1], [203, 0, 113, 1]];

async fn body(client: &Client, name: &str) {
    let apex = Name::from_ascii(format!("{name}.")).expect("SEEDCRYSTAL_CF_LIVE_NAME is a name");
    let mut cfg = PublishConfig::new(apex, Vec::new(), 1);
    cfg.per_name = 3;
    cfg.ttl = 60;
    cfg.dry_run = false;
    cfg.validate().unwrap();

    // One address from each RFC 5737 range: three network groups.
    let nodes = DOC_ADDRS
        .iter()
        .map(|&a| Servable {
            addr: SocketAddr::new(IpAddr::from(a), 23222),
            services: 1,
        })
        .collect();
    let snapshot = Snapshot {
        nodes,
        built_at: 1000,
    };
    let mut rng = StdRng::seed_from_u64(1);
    let report = reconcile_once(client, &cfg, &snapshot, &mut rng)
        .await
        .expect("reconcile");
    assert_eq!(report.created, 3, "{report:?}");
    assert!(report.foreign.is_empty(), "{report:?}");

    let records = client.records_for(name).await.expect("read back");
    let mut ips: Vec<IpAddr> = records
        .iter()
        .filter(|r| r.kind == RecordKind::A && r.comment.as_deref() == Some(OWNER_COMMENT))
        .map(|r| r.content)
        .collect();
    ips.sort();
    let expected: Vec<IpAddr> = DOC_ADDRS.iter().map(|&a| IpAddr::from(a)).collect();
    assert_eq!(ips, expected);
}

#[tokio::test]
#[ignore = "talks to the real Cloudflare API; needs the SEEDCRYSTAL_CF_LIVE_* env vars"]
async fn live_publish_and_cleanup() {
    let (Some(zone), Some(token), Some(name)) = (
        env("SEEDCRYSTAL_CF_LIVE_ZONE_ID"),
        env("SEEDCRYSTAL_CLOUDFLARE_TOKEN"),
        env("SEEDCRYSTAL_CF_LIVE_NAME"),
    ) else {
        eprintln!(
            "skipped: set SEEDCRYSTAL_CF_LIVE_ZONE_ID, SEEDCRYSTAL_CLOUDFLARE_TOKEN and \
             SEEDCRYSTAL_CF_LIVE_NAME to run the live Cloudflare test"
        );
        return;
    };
    let name = normalize_name(&name);
    assert!(
        name.split('.').count() > 1 && name.split('.').next() == Some(REQUIRED_LABEL),
        "refusing to run: SEEDCRYSTAL_CF_LIVE_NAME ({name}) must be `{REQUIRED_LABEL}.<zone>`: \
         its first label must be exactly `{REQUIRED_LABEL}`, under the zone, so it cannot be \
         a real seed name"
    );

    let client = Client::new(DEFAULT_API_BASE, &zone, token).unwrap();

    // Never adopt a name that already holds records: cleanup must only remove what this run made.
    let existing = client.records_for(&name).await.expect("initial listing");
    assert!(
        existing.is_empty(),
        "refusing to run: {name} already has {} A/AAAA record(s); use an unused name",
        existing.len()
    );

    // Run the body as its own task so a failed assertion still reaches the cleanup below.
    let client = Arc::new(client);
    let outcome = tokio::spawn({
        let client = Arc::clone(&client);
        let name = name.clone();
        async move { body(&client, &name).await }
    })
    .await;
    let cleaned = cleanup(&client, &name).await;
    if let Err(e) = &cleaned {
        eprintln!("cleanup failed, records may remain on {name}; delete them by hand: {e}");
    }
    if let Err(e) = outcome {
        std::panic::resume_unwind(e.into_panic());
    }
    cleaned.expect("cleanup failed");

    let left: Vec<_> = client
        .records_for(&name)
        .await
        .expect("final listing")
        .into_iter()
        .filter(|r| r.comment.as_deref() == Some(OWNER_COMMENT))
        .collect();
    assert!(left.is_empty(), "owned records remain on {name}: {left:?}");
}
