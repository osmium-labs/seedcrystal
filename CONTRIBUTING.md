# Contributing to Seedcrystal

Thanks for helping. Bug reports, coin presets, documentation fixes and code are all welcome.

## Reporting a bug

Open an issue with the version (`seedcrystal --version`, and `git rev-parse --short HEAD` if you
built from source), the
preset, the publish mode (authoritative or Cloudflare), what you expected and what happened.
The output of `seedcrystal doctor --config <your config>` usually says more than a description
can. Remove tokens before pasting a config or a log.

Security issues go through [SECURITY.md](SECURITY.md), not a public issue.

## Building and testing

The toolchain is pinned in `rust-toolchain.toml`; `rustup` picks it up automatically.

```sh
cargo build
cargo test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

CI runs the formatting, lint and test commands on every push to `main` and every pull request.
`cargo test` runs everything that needs nothing but the machine it runs on: fake peers, a fake
resolver and a fake Cloudflare API, all on localhost. Three suites are opt-in and marked
`#[ignore]`:

| Suite | Needs | How to run |
|---|---|---|
| `tests/e2e.rs` | Docker, about 2 GB of images | see [tests/e2e/README.md](tests/e2e/README.md) |
| `tests/cloudflare_live.rs` | a Cloudflare zone and an API token | see below |
| `tests/raptoreum_probe.rs` | a stock Raptoreum testnet node | see the module comment |

The end-to-end gate is the release check: a real, unmodified node with a fresh datadir has to
take its first peers from Seedcrystal's DNS answers.

The live Cloudflare test checks the real API once. It needs three environment variables:
`SEEDCRYSTAL_CF_LIVE_ZONE_ID`, `SEEDCRYSTAL_CLOUDFLARE_TOKEN` (Zone → DNS → Edit) and
`SEEDCRYSTAL_CF_LIVE_NAME`, an unused name in your zone whose first label is exactly
`seedcrystal-test` (for example `seedcrystal-test.example.org`). It creates three A records on
that name, one address from each RFC 5737 documentation range, reads them back and deletes them.
It refuses to run if the name has another form or already holds records, so after an
interrupted run, delete the leftover records by hand. Without the variables it prints "skipped"
and passes.

```sh
cargo test --test cloudflare_live -- --ignored --nocapture
```

## Adding a coin

A coin is a preset file in `presets/`, named `<id>.toml`; `build.rs` compiles every file there
into the binary. The fields are documented in the README.

1. Take every value from the coin's **public** source at a release tag or commit, not from
   another coin's preset or from memory.
2. Start the file with `# Every value is cited from <owner>/<repo>@<tag or commit>.` and put
   the file and line each value came from in a comment next to it. Where the value is not the
   obvious constant (a version floor that depends on a fork height, a service flag that comes
   from a function), say why in the comment.
3. Run `cargo test`; it checks that every built-in preset parses and carries the citation
   header.
4. If you can, run the coin through the end-to-end gate: a compose file and a seeder config
   under `tests/e2e/`, plus a `#[test]` in `tests/e2e.rs` (the module docs show how).

## Pull requests

- Keep a pull request to one change, with tests for the behaviour it adds or fixes.
- `cargo fmt`, `cargo clippy` and `cargo test` must pass.
- Commit messages follow `type(scope): summary`, e.g. `fix(crawler): ...`, `feat(cloudflare): ...`,
  `docs: ...`. The body says why, not what.

By contributing you agree that your contribution is licensed under the MIT License.
