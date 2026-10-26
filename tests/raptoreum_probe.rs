//! Real-node check: probes a stock Raptoreum testnet node with the `raptoreum-testnet` preset.
//!
//! ```text
//! docker build -f tests/e2e/raptoreum.Dockerfile -t seedcrystal-raptoreum-test tests/e2e
//! docker run -d --name seedcrystal-rtm-probe -p 127.0.0.1:10231:10230 seedcrystal-raptoreum-test \
//!   -testnet -listen=1 -dnsseed=0 -connect=0 -listenonion=0 -upnp=0 -printtoconsole=1
//! SEEDCRYSTAL_PROBE_TARGET=127.0.0.1:10231 cargo test --test raptoreum_probe -- --ignored --nocapture
//! docker rm -f seedcrystal-rtm-probe
//! ```

use std::net::SocketAddr;
use std::time::Duration;

use seedcrystal::coin::builtin;
use seedcrystal::p2p::{ProbeOptions, probe};

#[tokio::test]
#[ignore = "needs a running stock Raptoreum testnet node; see the module docs"]
async fn raptoreum_testnet_handshake_with_a_stock_node() {
    let target: SocketAddr = std::env::var("SEEDCRYSTAL_PROBE_TARGET")
        .expect("set SEEDCRYSTAL_PROBE_TARGET=host:port")
        .parse()
        .expect("SEEDCRYSTAL_PROBE_TARGET must be host:port");
    let preset = builtin("raptoreum-testnet").unwrap();
    let opts = ProbeOptions {
        handshake_timeout: Duration::from_secs(10),
        addr_wait: Duration::from_secs(1),
        ..ProbeOptions::default()
    };
    let result = probe(target, &preset, &opts).await.expect("handshake");
    println!(
        "peer version {} agent {:?} services {:#x} height {}",
        result.peer.version, result.peer.user_agent, result.peer.services, result.peer.start_height
    );
    assert!(result.peer.version >= preset.min_peer_version);
}
