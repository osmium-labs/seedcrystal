# Security policy

Seedcrystal answers DNS queries from the internet, talks to untrusted P2P peers and can hold a
Cloudflare API token, so security reports matter to us.

## Reporting a vulnerability

Please report it privately through GitHub: on this repository's **Security** tab, choose
**Report a vulnerability**. Do not open a public issue.

Include what you found, how to reproduce it, and the version or commit you tested. We will
acknowledge the report, keep you informed while we work on a fix, and credit you in the release
notes unless you would rather not be named.

## Supported versions

Fixes go into the latest release and the `main` branch.

## In scope

- Anything that lets a remote party crash the seeder, exhaust its memory or CPU, or make it
  answer with addresses it did not crawl (DNS or P2P input).
- Anything that leaks the Cloudflare API token, or makes Seedcrystal change records it did not
  create.
- Ways to make the seeder serve nodes on the wrong chain despite a configured checkpoint.
