# Seedcrystal

**A multi-coin DNS seeder for Bitcoin-protocol UTXO chains.** Seedcrystal crawls your coin's
P2P network, keeps score of which nodes are reliable and on the right chain, and hands the good
ones to new nodes over DNS, from its own authoritative server or through Cloudflare.

[![CI](https://github.com/osmium-labs/seedcrystal/actions/workflows/ci.yml/badge.svg)](https://github.com/osmium-labs/seedcrystal/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust 1.98](https://img.shields.io/badge/rust-1.98-orange.svg)](rust-toolchain.toml)

Seedcrystal is built by [Osmium](https://www.osmium.space), as the first of a set of tools for
UTXO chains; [docs/osmium.md](docs/osmium.md) tells how it came about and where it is going.

- [Why](#why)
- [How it works](#how-it-works)
- [Supported coins](#supported-coins)
- [Install](#install)
- [Quickstart: your own nameserver](#quickstart-your-own-nameserver)
- [Quickstart: Cloudflare](#quickstart-cloudflare)
- [Running it in production](#running-it-in-production)
- [Checking a seed: `doctor`](#checking-a-seed-doctor)
- [Exporting fixed seeds: `seeds`](#exporting-fixed-seeds-seeds)
- [Configuration reference](#configuration-reference)
- [Adding a coin: preset reference](#adding-a-coin-preset-reference)

## Why

A fresh node knows no peers. It finds its first ones through DNS: it looks up
`x<service bits>.<seed name>` and connects to the addresses in the answer. If the seeder
publishes the wrong names, or stale and dead nodes, new nodes fall back to a short list of
hard-coded addresses. Seedcrystal is built to get this right the first time:

- **It answers the names nodes actually query.** Every `x<hex>` service filter is answered,
  computed from the coin's own desirable service flags, not Bitcoin's `x9` copied into every coin.
- **Coins are configuration, not forks.** One TOML preset per network: magic, port, protocol
  versions, service bits. Each built-in preset cites the line of the coin's source that every
  value came from.
- **It serves only nodes worth connecting to.** Reliability is scored over five windows, from
  two hours to a month. A chain-tip height floor ignores a single node lying about its height,
  an optional checkpoint keeps forks out, and each answer holds at most two addresses per IPv4
  /16 or IPv6 /32.
- **Two ways to publish.** A built-in authoritative DNS server (UDP and TCP, no separate DNS
  software), or Cloudflare mode, which keeps A and AAAA records in your zone and never touches
  records it did not create.
- **It checks itself.** `seedcrystal doctor` resolves your seed through public resolvers the
  way a node does, and says what to fix when something is wrong.
- **It feeds your fixed seeds.** `seedcrystal seeds` prints the best nodes in the input format
  of a coin's `contrib/seeds/generate-seeds.py`.

## How it works

1. **Crawl.** Starting from the coin's own seeds (or peers you list), Seedcrystal connects to
   nodes, 64 at a time, completes the P2P handshake and asks for more addresses. Good nodes are
   checked again every 15 minutes; failing ones back off from 5 minutes up to a day.
2. **Score.** Every check updates the node's reliability over five windows (2 hours to a month).
   A node *meets the full bar* when any one window shows a high enough success rate over enough
   checks; three successful checks ten minutes apart are enough. To be *servable* it must also
   run a recent enough protocol version, be no more than `tip_tolerance` blocks behind the
   network's tip (about two hours of blocks), and, if the preset sets a checkpoint, be on that
   chain.
3. **Serve.** Every 30 seconds the servable nodes are collected into a snapshot, which is what
   DNS answers come from, up to 20 addresses per answer. While fewer than 10 nodes meet the full
   bar (on a fresh install, for example), *warm-up* nodes are served too: nodes that completed a
   handshake in the last hour. A new seeder is therefore useful within minutes.

There are two ways to publish the snapshot. Pick one per seed name:

| | Authoritative (default) | Cloudflare |
|---|---|---|
| Who answers DNS | Seedcrystal itself, on port 53 | Cloudflare, from records Seedcrystal maintains |
| You need | A server with a public IP and two DNS records pointing at it | A Cloudflare zone holding the seed name and an API token |
| Answers | A fresh random selection on every query | The records published in the last cycle (every 5 minutes) |
| Setup | [Quickstart: your own nameserver](#quickstart-your-own-nameserver) | [Quickstart: Cloudflare](#quickstart-cloudflare) |

## Supported coins

| Preset | Coin | Network | P2P port |
|---|---|---|---|
| `bitcoin-mainnet` | Bitcoin | mainnet | 8333 |
| `bitcoin-testnet4` | Bitcoin | testnet4 | 48333 |
| `osmium-mainnet` | Osmium | mainnet | 9969 |
| `osmium-testnet` | Osmium | testnet | 19969 |
| `raptoreum-mainnet` | Raptoreum | mainnet | 10226 |
| `raptoreum-testnet` | Raptoreum | testnet | 10230 |

`seedcrystal presets` lists them with their port, seed name prefix and protocol version. Osmium and Raptoreum pass the end-to-end release gate: a stock
node with an empty data directory takes its first peers from Seedcrystal. Another coin needs
only a preset file; see [Adding a coin](#adding-a-coin-preset-reference).

## Install

Seedcrystal is built from source and runs on Linux. You need `git`, a C compiler (the
`build-essential` or `gcc` package; SQLite is compiled in) and Rust from
[rustup](https://rustup.rs). The exact Rust toolchain is pinned in `rust-toolchain.toml` and
fetched automatically on the first build.

```sh
git clone https://github.com/osmium-labs/seedcrystal
cd seedcrystal
cargo build --release
sudo install -m 755 target/release/seedcrystal /usr/local/bin/seedcrystal
```

The binary is all you need. To run it as a service, see
[Running it in production](#running-it-in-production).

## Quickstart: your own nameserver

This runs Seedcrystal as the authoritative DNS server for your seed name. The example seeds
Osmium as `dnsseed.osmium.space`; use your own names. The server needs a public IP address,
inbound UDP and TCP on port 53, and outbound TCP to the coin's P2P port (that is how it checks
nodes).

1. **Write a config.** Copy the example and replace the three names and the preset with yours:

   ```sh
   cp examples/osmium.toml seedcrystal.toml
   ```

   ```toml
   [[network]]
   preset = "osmium-mainnet"
   seed = "dnsseed.osmium.space"          # the name your nodes will query
   nameserver = "seed-ns.osmium.space"    # this server's own name, outside the seed zone
   mailbox = "hostmaster.osmium.space"    # contact for the zone's SOA record (an email address also works)
   data_dir = "/var/lib/seedcrystal"      # crawl database lives here
   ```

2. **Delegate the seed name to this server.** In the parent zone (`osmium.space` here), at your
   registrar or DNS host, create:

   | Name                    | Type | Value                                  |
   |-------------------------|------|----------------------------------------|
   | `seed-ns.osmium.space`  | A    | the public IPv4 address of this server |
   | `dnsseed.osmium.space`  | NS   | `seed-ns.osmium.space`                 |

   Add an AAAA record for `seed-ns` as well if the server has a public IPv6 address.

3. **Prepare the server.** Create the data directory, and let the binary bind port 53 without
   running as root (the systemd unit in [production](#running-it-in-production) does both for
   you):

   ```sh
   sudo install -d -o "$USER" /var/lib/seedcrystal
   sudo setcap 'cap_net_bind_service=+ep' /usr/local/bin/seedcrystal
   ```

   Open UDP and TCP port 53 in the firewall or cloud security group. On Ubuntu and Debian,
   `systemd-resolved` already listens on `127.0.0.53:53`, and Seedcrystal's default wildcard
   listener then fails with "address already in use" (`sudo ss -lunp 'sport = :53'` shows who
   holds the port). Either set `DNSStubListener=no` in `/etc/systemd/resolved.conf`, or list only
   the server's public address in `dns.listen`. To try Seedcrystal first on an unprivileged port,
   set `SEEDCRYSTAL_DNS_LISTEN=127.0.0.1:5353`.

4. **Run it:**

   ```sh
   seedcrystal run --config seedcrystal.toml
   ```

   Logs go to stderr; set `RUST_LOG=debug` for more. `SIGINT` and `SIGTERM` stop it cleanly. A
   cold seeder starts empty and answers with an empty (but valid) reply until the crawl has
   found nodes, usually within a minute or two; until then nodes fall back to their other seeds.

5. **Check it.** Ask the server directly, then from the outside with `doctor`:

   ```sh
   dig @<server-ip> x1.dnsseed.osmium.space          # A records of full nodes
   dig @<server-ip> x1.dnsseed.osmium.space AAAA
   dig @<server-ip> dnsseed.osmium.space NS
   seedcrystal doctor --config seedcrystal.toml
   ```

   `x1` asks for nodes with service flag 1 (the prefix is `x` plus the hex service bits); the
   plain `dnsseed.osmium.space` name works too. Once the NS record has propagated, `dig` works
   without `@<server-ip>`.

6. **Point your nodes at it.** A node queries only the seed names compiled into it (`vSeeds` in
   the coin's `chainparams.cpp`). If the name is already there, new nodes use the seeder as soon
   as the delegation is live; otherwise it takes a node release that adds it.

## Quickstart: Cloudflare

If the seed name's zone is on Cloudflare, Seedcrystal can publish the node addresses as ordinary
A and AAAA records there instead of running its own DNS server. Use it when you cannot or do not
want to open port 53 or run a nameserver: Cloudflare serves the answers. No `dns.listen`,
`nameserver` or `mailbox` is needed; the server only needs outbound HTTPS and outbound TCP to the
coin's P2P port.

The seed name must be an ordinary name inside the zone, with no NS or CNAME record on it.
Seedcrystal only ever touches A and AAAA records on the plain seed name and its `x<hex>` names.

1. **Create an API token** (Cloudflare dashboard, My Profile, API Tokens) with the permission
   **Zone → DNS → Edit**, scoped to the one zone that contains the seed name. Add
   **Zone → Analytics → Read** if you want `doctor` to check that resolvers are asking.
2. **Find the zone ID** on that zone's Overview page, in the "API" section.
3. **Delete any hand-made A/AAAA records on the published names** (the plain seed name and
   `x1`, `x5`, ... for Osmium). Seedcrystal never touches records it did not create, so they
   would be mixed into every answer; it logs a warning while they exist.
4. **Store the token** in a file only the seeder can read (`chmod 600`), or export it as
   `SEEDCRYSTAL_CLOUDFLARE_TOKEN`. Setting both is an error.
5. **Write a config** from the example and create the data directory:

   ```sh
   cp examples/osmium-cloudflare.toml seedcrystal.toml
   sudo install -d -o "$USER" /var/lib/seedcrystal
   ```

   ```toml
   [[network]]
   preset = "osmium-mainnet"
   seed = "dnsseed.osmium.space"          # must lie inside the Cloudflare zone
   data_dir = "/var/lib/seedcrystal"
   publish = "cloudflare"

   [network.cloudflare]
   zone_id = "0123456789abcdef0123456789abcdef"
   token_file = "/etc/seedcrystal/cloudflare.token"
   dry_run = true                         # first run: only log what would change
   ```

6. **Run it once in dry-run mode:** `seedcrystal run --config seedcrystal.toml`. The log lists
   every record it would create or delete. When that looks right, set `dry_run = false` (or
   remove the key) and restart.
7. **Check it** with `seedcrystal doctor --config seedcrystal.toml`. A name that was NXDOMAIN
   stays cached by resolvers for up to the zone's SOA minimum TTL, so right after the first
   publish step 2 can fail for a while; wait and run it again.
8. **Point your nodes at it**, as in step 6 of the [nameserver quickstart](#quickstart-your-own-nameserver).

**Run one publisher per zone.** Two Seedcrystal instances, or one plus an old uploader that
still writes the same names, delete each other's records on every cycle. Stop the old one first.

### How Cloudflare mode manages records

- **What it publishes.** For the plain seed name and each filter name of the preset (`x1`, `x5`
  for Osmium): up to `per_name` (default 20) A records and as many AAAA records, with TTL 60 s
  and the comment `managed by seedcrystal`.
- **Ownership.** Only records with exactly that comment are ever changed or deleted. Remove the
  comment in the dashboard and the record becomes yours: Seedcrystal leaves it alone and warns.
  Owned records with the wrong TTL, or proxied through Cloudflare, are replaced.
- **Gradual changes.** A record is replaced only when its node stops qualifying, so a changing
  crawl does not churn the zone. On a name that already holds `per_name` records, two records per
  family are swapped each cycle for nodes not yet published, which spreads bootstrap traffic. A
  name the group limit leaves short of `per_name` is not rotated: a swap there could only trade
  one address of a full group for another of the same group.
- **Network groups.** A name never holds more than two addresses of one network group (an IPv4
  /16 or an IPv6 /32): a node opens one outbound connection per group, so a concentrated answer
  would leave it fewer usable choices. With too few groups, fewer addresses are published. Of
  owned records over a group's limit, the lowest addresses stay.
- **Shrink guard.** If the crawl has no nodes, existing records stay as they are. If the
  servable count drops below half of its highest value in the last 24 hours, a cycle only fills
  gaps: it deletes and rotates nothing, and logs a warning until the crawl recovers.
- **Errors.** If Cloudflare refuses the token (HTTP 401 or 403) the seeder stops with a message
  naming the missing permission. Rate limits and server errors are retried with a growing delay.
- **Record quota.** A Cloudflare Free zone may limit the total number of records. Seedcrystal
  creates up to `per_name × 2 families × published names` records: 20 × 2 × 3 = 120 with the
  Osmium defaults. Check the zone's quota before the first run.

## Running it in production

[`contrib/systemd/seedcrystal.service`](contrib/systemd/seedcrystal.service) runs Seedcrystal as
an unprivileged system service that may bind port 53 and write only to `/var/lib/seedcrystal`.
Its header lists the install commands. Logs then go to the journal:
`journalctl -u seedcrystal`. The config and data directory belong to the service user, so run
the other commands as that user:

```sh
sudo -u seedcrystal seedcrystal doctor --config /etc/seedcrystal/seedcrystal.toml
sudo -u seedcrystal seedcrystal seeds --config /etc/seedcrystal/seedcrystal.toml
```

- **Several seeds, one process.** A config may hold several `[[network]]` tables, each with its
  own seed name and preset, crawled independently.
- **Hosts with several addresses** (a cloud floating or reserved IP, several IPv6 addresses):
  list the addresses of the nameserver's A/AAAA records explicitly in `dns.listen` instead of
  the wildcard default. A wildcard UDP socket replies from whichever source address the kernel picks, and
  resolvers drop replies from an address they did not query.
- **Upgrading.** Stop the service, replace the binary, start it again. The crawl database is
  kept. A build refuses a database written by a newer one, so an accidental downgrade cannot
  damage it. Without the systemd unit, run `setcap` again after replacing the binary: the
  capability belongs to the file.

Files in `data_dir`, one set per network:

| File | What it holds |
|---|---|
| `<preset id>.sqlite` | The crawl database: every known node, its score and its last check. |
| `queries-<preset id>.json` | Authoritative mode: answered and empty A/AAAA queries per name, written every 60 s and at shutdown. `doctor` reads it. |
| `shrink-guard-<preset id>.json` | Cloudflare mode: the last 24 hours of servable counts, so a restart during a crawl collapse keeps the guard's high-water mark. |

The query log looks like
`{"since": <unix>, "names": {"x1.dnsseed.osmium.space": {"answered": n, "empty": n, "last_seen": <unix>}}}`.
Its counters start over when the seeder restarts (`since` shows when), and names beyond 1024
that resolvers invented are folded into one `_other` entry so they cannot crowd out the real
ones.

## Checking a seed: `doctor`

`seedcrystal doctor --config <path> [--resolver ip[:port]]...` checks each configured seed from
the outside, the way a coin node resolves it, and exits 1 if any check fails (a SKIP does not
fail). It asks recursive resolvers (default 1.1.1.1, 8.8.8.8 and 9.9.9.9; repeat `--resolver`
to choose your own), asking them to recurse, 3 s per query, retrying over TCP when the answer is truncated.
A check passes only if every resolver that answered agrees; a resolver that times out is named
in the detail but does not fail the check on its own.

1. **Delegation / records.** Authoritative: the seed's NS equals `nameserver`, and that name
   resolves. Cloudflare: the zone holds Seedcrystal-owned records for the published names, and
   no foreign records on them.
2. **What a node resolves.** The plain name and `x<seed_flags>.<seed>` (`x1` for Osmium, the
   exact name the node asks for) return addresses; AAAA may be empty but not NXDOMAIN or
   SERVFAIL.
3. **The answers are real nodes.** Up to five returned addresses get a protocol handshake on the
   preset's port; one success passes.
4. **Real resolvers are asking.** Authoritative: the query log shows an answered query for the
   plain or `x` name within 24 hours (your own `dig` and `doctor` runs count). Cloudflare: the
   zone's DNS analytics for the last 24 hours show the plain or `x` name was queried. This needs
   the token's Zone → Analytics → Read permission; without it the step is skipped, naming
   Cloudflare's message. Counts are sampled estimates and include your own lookups. Cloudflare
   answers NOERROR even for names that do not exist, so step 4 shows that resolvers *ask*; step
   2 shows that the answers carry addresses. The detail lists every name the check covers, with
   the day and hour of the latest sampled queries (UTC), and `~0 queries` for a silent one. If
   Cloudflare reports a GraphQL error alongside usable rows, the rows are used and the detail
   ends with `(Cloudflare also reported: <message>)`.

Run `doctor` from the seeder's directory, or give `data_dir` as an absolute path: a relative one
is resolved against the directory `doctor` runs in, and step 4 would not find the query log.

A healthy seed:

```
dnsseed.osmium.space (osmium-mainnet)
PASS  1  delegation — dnsseed.osmium.space is delegated to seed-ns.osmium.space, which resolves, at 3 resolver(s)
PASS  2  what a node resolves — x1.dnsseed.osmium.space and dnsseed.osmium.space resolve at 3 resolver(s), 12 distinct address(es)
PASS  3  the answers are real nodes — 5 of 5 answered the handshake: ...
PASS  4  real resolvers are asking — x1.dnsseed.osmium.space answered 41 time(s), last 3 min ago; the count includes your own dig and doctor runs
```

A seeder that publishes the plain name but not the one nodes ask for:

```
PASS  1  delegation — ...
FAIL  2  what a node resolves — A/AAAA x1.dnsseed.osmium.space -> NXDOMAIN via 1.1.1.1:53, 8.8.8.8:53, 9.9.9.9:53
  fix: your node queries `x1.dnsseed.osmium.space`, which returns NXDOMAIN: publish that name (via 1.1.1.1:53, 8.8.8.8:53, 9.9.9.9:53)
PASS  3  the answers are real nodes — ...
PASS  4  real resolvers are asking — ...
```

In Cloudflare mode step 4 reads, for example:

```
PASS  4  real resolvers are asking — x1.dnsseed.osmium.space ~40 queries, last 10-25 15:00 UTC; dnsseed.osmium.space ~0 queries (sampled; includes your own lookups)
```

## Exporting fixed seeds: `seeds`

Coins compile a list of fixed seed nodes (`chainparamsseeds.h`) into the node as a fallback, and
regenerate it with `contrib/seeds/generate-seeds.py`. `seeds` writes that script's input:

```sh
seedcrystal seeds --config seedcrystal.toml > nodes_main.txt
```

- One `ip:port` per line, IPv6 as `[addr]:port`. Osmium's and Raptoreum's `generate-seeds.py`
  both accept this format.
- `--max` limits the list (default 50). `--network <seed name>` picks the network when the
  config has more than one.
- The list comes from the same snapshot policy as `run`, so it never disagrees with the running
  seeder: nodes that meet the full bar first, then warm-up ones, each sorted by address text, at
  most 2 per IPv4 /16 or IPv6 /32.
- It reads the crawl database and can run while the service is running. It runs no SQL that
  writes, fails if there is no database, and refuses one whose schema differs from this build's.
  If a crashed service left a `-wal` file and `seeds` is the only connection, SQLite folds that
  file into the database when `seeds` closes, the same recovery `run` does on start.

## Configuration reference

The config is a TOML file with an optional `[dns]` table and one `[[network]]` table per seed.
Unknown keys are rejected with the key's name.

| Key | Required | Meaning |
|-----|----------|---------|
| `dns.listen` | no | Addresses the authoritative server listens on, UDP and TCP. Default `["0.0.0.0:53", "[::]:53"]`. Unused when every network publishes to Cloudflare. |
| `network.preset` / `network.preset_file` | one of them | A built-in preset id, or a path to your own preset file. A relative path is taken from the config file's directory. |
| `network.seed` | yes | The seed hostname this network answers for. |
| `network.publish` | no | `"authoritative"` (default): Seedcrystal's own DNS server answers. `"cloudflare"`: records are published into a Cloudflare zone. |
| `network.nameserver` | authoritative | Public name of this server; must be outside the seed zone. |
| `network.mailbox` | authoritative | Contact for the zone's SOA record, as a DNS name (`hostmaster.example.org`) or an email address. |
| `network.data_dir` | yes | Directory for the crawl database and the files listed under [production](#running-it-in-production). Two networks with the same preset need different directories. |
| `network.bootstrap` | no | Extra `ip:port` peers to start crawling from. Each must use the preset's default port and a publicly routable address, because the store drops anything else; the config is rejected otherwise, naming the entry, unless `allow_private = true`. |
| `network.allow_private` | no | Crawl and serve private/loopback addresses, and lift the port restriction so that any port is probed and served. For tests and private networks only. |
| `network.cloudflare.zone_id` | Cloudflare | The Cloudflare zone's ID. Only valid with `publish = "cloudflare"`. |
| `network.cloudflare.token_file` | Cloudflare, or the env var | File holding the API token (surrounding whitespace ignored; keep it `chmod 600`, a warning is logged otherwise). There is no inline `token` key. |
| `network.cloudflare.ttl` | no | TTL of published records in seconds, at least 60. Default 60. |
| `network.cloudflare.interval_secs` | no | Seconds between publish cycles. Default 300. |
| `network.cloudflare.per_name` | no | Addresses published per name and family, 1 to 50. Default 20. |
| `network.cloudflare.dry_run` | no | Log the changes a cycle would make without making them. Default false. |
| `network.cloudflare.api_base` | no | Cloudflare API URL. Default `https://api.cloudflare.com/client/v4`; change it only for tests. |

Environment variables:

| Variable | Effect |
|---|---|
| `SEEDCRYSTAL_DNS_LISTEN` | Overrides `dns.listen`, comma-separated (`127.0.0.1:5353`). |
| `SEEDCRYSTAL_CLOUDFLARE_TOKEN` | The Cloudflare API token, instead of `token_file` (not both). |
| `RUST_LOG` | Log level, for example `debug` or `seedcrystal=debug`. Default `info`. |
| `NO_COLOR` | Any non-empty value turns off coloured output. Output is only coloured on a terminal. |

## Adding a coin: preset reference

A coin network is a preset file. To use one, point `preset_file` at it; to make it a built-in,
drop it into `presets/` as `<id>.toml` and rebuild. No code changes either way. A preset for a
coin other people use is a welcome contribution; see [CONTRIBUTING.md](CONTRIBUTING.md).

1. **Start from the closest shipped preset.** `bitcoin-mainnet` for a coin that follows Bitcoin
   Core, `osmium-mainnet` for a Dash-derived coin with masternodes. Copy it and change `id`; the
   id also names the files in `data_dir`.
2. **Take every value from your coin's source** at a release tag, not from memory or another
   coin. The shipped presets cite the file and line of each value in comments; that is the
   quickest map of where to look:

   | Field | Where it usually lives |
   |---|---|
   | `magic` | `pchMessageStart` in `chainparams.cpp`, the four bytes in order as hex (`"4f534d49"`) |
   | `default_port` | `nDefaultPort` in `chainparams.cpp` |
   | `protocol_version`, `min_peer_version` | `PROTOCOL_VERSION`, `MIN_PEER_PROTO_VERSION` in `version.h` (check `net_processing.cpp` for height-dependent floors) |
   | `addrv2_min_version` | the version that introduced `sendaddrv2` (BIP155); omit it if the coin has no `sendaddrv2` |
   | `block_interval_secs` | `nPowTargetSpacing` in `chainparams.cpp`, or the effective spacing if it changes with height |
   | `seed_flags` | what `GetDesirableServiceFlags(NODE_NONE)` returns (`protocol.cpp`/`protocol.h`); the node's DNS lookup in `net.cpp` uses it as `x%x.<seed>` |
   | `bootstrap_dns` | the `vSeeds` names in `chainparams.cpp` |
   | `bootstrap_fixed` | `chainparamsseeds.h`, decoded to `ip:port` |
   | `version_trailer` | `"dash"` if the coin's `version` message carries a masternode challenge after the relay byte (Dash forks), otherwise `"none"` |

3. **Mind the types.** `magic` is a string of 8 hex digits; `seed_flags` and `filters` are TOML
   integers (`0x1`, `[0x1, 0x5]`). `filters` lists the `x<hex>` names Cloudflare mode publishes:
   it must create each name in advance, while the authoritative server answers any `x<hex>` name
   anyway. Include `seed_flags` and the service combinations your nodes commonly advertise.
4. **Test it.** Point a config at the file with `preset_file`, add one of your nodes to
   `bootstrap`, and run with `RUST_LOG=debug`. A handshake that fails points at `magic`,
   `version_trailer`, `checksum`, `protocol_version` or the `addrv2_*` fields.

Built-in presets must start with `# Every value is cited from <owner>/<repo>@<tag or commit>.`
(`cargo test` checks it). In your own `preset_file`, nothing reads the comments, but citing your
sources the same way makes the values easy to check later.

Optional fields that need a syntax example:

```toml
checkpoint = { height = 120000, hash = "<64 hex digits, as block explorers show it>" }
serve_version_schedule = [{ height = 420420, version = 70219 }]
```

| Field | Default | Meaning |
|---|---|---|
| `id` | required | Preset name; for built-ins it must match the file name. |
| `magic` | required | Network magic, 8 hex digits. |
| `default_port` | required | The coin's P2P port. |
| `protocol_version` | required | Version we send in `version`. Keep it at or above every floor the chain's nodes enforce. |
| `min_peer_version` | required | Handshake floor: peers below it are disconnected. |
| `version_trailer` | required | `"none"`: `version` ends after the relay byte. `"dash"`: 32 random bytes, then one `0x00` flag (we never claim to be a masternode). |
| `addrv2_min_version` | absent | First protocol version with BIP155 (`addrv2`). Absent: `sendaddrv2` is never sent. |
| `addrv2_placement` | `"before_verack"` | `"after_verack"` sends `sendaddrv2` right after our `verack`, for nodes that expect it there. |
| `checksum` | `"sha256d"` | P2P header checksum; `"groestl512d"` is double Groestl-512. |
| `block_interval_secs` | required | The effective block spacing in seconds, > 0. |
| `tip_tolerance` | `max(6, ceil(7200 / block_interval_secs))` | Blocks a node may trail the network's tip and still be served (two hours of blocks). |
| `min_serve_version` | `min_peer_version` | Lowest version that is served; must be at least `min_peer_version`. Older peers are still crawled but not served. |
| `serve_version_schedule` | empty | List of `{ height, version }`. The floor at tip height `h` is the highest of `min_serve_version` and every entry with `height <= h`. Heights strictly ascending, versions non-decreasing. |
| `min_height` | `0` | Nodes below this height are never served. |
| `checkpoint` | absent | `{ height, hash }`: a block of your chain from after any fork or relaunch that shares its magic and port, so nodes of the other chain are not served. |
| `addr_wait_secs` | `30` | Upper bound on `getaddr` collection. |
| `seed_flags` | required | Service bits in the `x<hex>` name the coin's nodes query. |
| `filters` | required | `x<hex>` names Cloudflare mode publishes; must contain `seed_flags`. |
| `bootstrap_dns`, `bootstrap_fixed` | required | Where the first crawl starts (either may be empty). |

## Roadmap

Planned: exporting the answers as a zone file for an existing DNS server, pushing them to one
with RFC 2136 dynamic updates, and DNSSEC signing.

## Contributing

Bug reports, coin presets and pull requests are welcome; [CONTRIBUTING.md](CONTRIBUTING.md) has
the details, including the opt-in tests against Docker, a live node and the real Cloudflare API.
Please report security issues privately, as described in [SECURITY.md](SECURITY.md).

## License

MIT, see [LICENSE](LICENSE).
