# Seedcrystal and Osmium

Seedcrystal is built and maintained by [Osmium](https://www.osmium.space), a proof-of-work
cryptocurrency with masternodes and on-chain governance, built on the Dash codebase. Osmium's
mainnet launched in April 2024.

## Where Seedcrystal came from

A new Osmium node finds its first peers the way Bitcoin's nodes do: it looks up
`x1.dnsseed.osmium.space` and connects to the addresses in the answer. The seeder behind that
name was a widely used open-source one, and in the mode Osmium ran it in, it published only
Bitcoin's `x9` name. Osmium Core never asks for `x9`, so every result the crawler produced was
invisible to the nodes it was meant for, and new nodes fell back to three hard-coded addresses.

Fixing this for Osmium meant writing a seeder that answers the names a node actually queries.
Writing it for every Bitcoin-protocol coin cost little more, so Seedcrystal was built multi-coin
from the first commit. It serves Osmium's own seed.

## Where it is going

The [Osmium whitepaper](https://whitepaper.osmium.space) states the project's purpose: "to
promote and reward talent in the blockchain space." Part of that is infrastructure. Most UTXO
coins are forks of Bitcoin or Dash, and the seeders, explorers, pools and miners around them were
usually written for one coin, with its parameters compiled in; each new chain forks and patches
them again. Osmium aims to build these tools once, for every chain that speaks the Bitcoin
protocol. Seedcrystal is the first.

If you run a UTXO chain that Seedcrystal does not support yet, a preset is usually all it takes;
see [Adding a coin](../README.md#adding-a-coin-preset-reference).
