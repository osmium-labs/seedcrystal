# End-to-end gate: a real node seeds itself from Seedcrystal

This is the release gate. It proves the one thing a DNS seeder is for: a real, unmodified node
with a fresh, empty datadir asks its usual seed name and takes its first peers from
Seedcrystal's answers.

For Osmium the stack is:

| Service  | Address        | Role |
|----------|----------------|------|
| `node1..3` | 11.99.0.11–13 | Osmium v1.2.0 testnet nodes from the official release tarball, connected to each other, DNS seeding off |
| `seeder` | 11.99.0.2      | This repository's binary, serving `testnet-seed.osmium.space` and crawling from node1 |
| `client` | 11.99.0.20     | A fresh Osmium node with default `-dnsseed`, whose system resolver is the seeder |

The client starts only once the seeder's readiness check sees a crawled node in its answers
(it asks for `x405`, the exact service bits the test nodes advertise, and deliberately not
`x1`: a seeder that answers `x1` wrongly still starts the client and then fails on the client's
log). On an empty address book the client queries `x1.testnet-seed.osmium.space`. The
gate passes when the client's `debug.log` reports `N addresses found from DNS seeds` with N > 0
**and** `getnodeaddresses 0` lists at least one of the three nodes (all three share a /16, and
an answer holds at most two of one group, so each answer carries two of them). A merely
non-empty address book is not a pass: a node adds its fixed seeds after 60 s with an empty one.

Raptoreum has its own stack in `raptoreum-compose.yml`, on the same addresses with the nodes
at 11.99.0.21–23: Raptoreum Core 2.0.3.01 testnet nodes from the official release tarball,
the seeder serving `lbtn.raptoreum.com` (the node's compiled-in testnet seed name; its other
seed is a bare IP, which the seeder never answers), and a fresh client querying
`x1.lbtn.raptoreum.com`. The pass criterion is the same except for the second half:
Raptoreum's `getnodeaddresses` cannot be used (its CLI sends the count as a string, it
rejects 0, and it returns at most 23% of the address manager, which is nothing below five
entries). The client has no `addnode`, so a `getpeerinfo` entry at a node's address is a peer
it took from DNS.

The network is `internal: true`, so no container can reach the internet, and it uses
11.99.0.0/24, a range address managers do not discard as private. While the stack runs, the
host's route for that range points at the test network. The fixed range also means only one
gate runs at a time per host.

## Running it

It needs Docker with Compose v2 and about 2 GB of images (the Rust build image, Debian, the
Osmium release). The first run downloads and builds them; later runs reuse them.

```sh
cargo test --test e2e -- --ignored --test-threads=1 --nocapture
cargo test --test e2e raptoreum -- --ignored --test-threads=1 --nocapture   # one coin
```

The test brings the stack up under a unique compose project name, polls the client every 5 s
for up to 6 minutes, prints the seeder log, the seeder's answer for the queried name, the
client's `debug.log` and its `getnodeaddresses` output (`getpeerinfo` for Raptoreum), then tears
everything down
(`docker compose down -v`), also when it fails or panics. A watchdog tears down and fails the
test at 8 minutes, image builds included. On a slow link, build the images first so the
first run fits in that cap:

```sh
docker compose -f tests/e2e/docker-compose.yml build
docker compose -f tests/e2e/raptoreum-compose.yml build
```

Reading a failure:

- `Waiting … before querying DNS seeds` in the client log: the datadir was not fresh.
- `0 addresses found from DNS seeds`: the seeder answered the client's name empty or with an
  error. Compare the seeder's answer in the dump with the name the client asked.
- No DNS seed line at all: the client's resolver path is broken.

## When to run it

By hand, before each release, and after any change to `p2p`, `dns` or the presets. It is not
part of `cargo test` or CI.

Adding a coin means adding a compose file and a seeder config here, and a `Gate` plus a
`#[test]` in `tests/e2e.rs`.
