# zkas-pool

[![License: MIT or Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust 1.91+](https://img.shields.io/badge/rust-1.91%2B-orange.svg)](Cargo.toml)

Rust-first **[ZKas](https://github.com/firecash/firecash-rusty)** mining pool — a
fork of [Nacho-the-Kat/katpool](https://github.com/Nacho-the-Kat/katpool) retargeted at
the shielded-by-default ZKas chain. A single-binary deployment that owns stratum,
share validation, block submission (native **and** AuxPoW merged mining), PROP
accounting, and **shielded `$zkas` payouts** — backed by `PostgreSQL`.

Because ZKas's PoW is kHeavyHash (byte-identical to Kaspa), the stratum/mining path
is unchanged from a Kaspa pool. What is different is **payout**: ZKas has no
transparent UTXOs — the coinbase reward is minted as an Orchard shielded note — so
custodial mode requires a **shielded treasury** with automatic shielded payouts
(see below).

> The internal crate and binary names are still `katpool` / `katpool-*`, inherited from
> the upstream fork; they are not renamed to avoid churning 60+ patched `kaspa-*` deps.
> The product is ZKas; the binary you build is `katpool`.

## Status

> **Live today: direct (solo-style) payout.** The stratum **bridge** connects to a ZKas
> node and mines native + AuxPoW solutions; each block template pays the coinbase to the
> **address the finding miner authorized with**, so rewards land on-chain directly —
> the pool holds nothing. The **custodial path is being built** (shielded treasury →
> PROP accounting → automatic shielded payouts), replacing the upstream
> transparent-KAS/KRC-20 payout engines, which do **not** apply to ZKas.
> Do not enable custodial mode (`coinbase_address_override`) until that path lands.

## At a glance

**Runtime** (what `cargo build --release --bin katpool` ships):

| Component | Responsibility |
|---|---|
| [`bridge/`](bridge/) | Forked `rusty-kaspa` stratum bridge. Accepts ASIC stratum, validates shares, submits native + AuxPoW blocks to a ZKas node. **Works today.** |
| [`accountant/`](accountant/) | Subscribes to share + block events, computes PROP allocations, writes per-miner balances. |
| [`payout-zkas/`](payout-zkas/) | **The ZKas payout engine.** Periodic full-balance sweeps from the shielded treasury — one Orchard tx per recipient via the `shielded-pay` CLI, single-leader, idempotent, spend-capped, with an in-flight double-pay latch. |
| [`payout-kas/`](payout-kas/) | **Upstream transparent-KAS payout — NOT used by ZKas** (no transparent UTXOs). Kept for upstream parity; superseded by `payout-zkas`. |
| [`payout-krc20/`](payout-krc20/) | **Upstream NACHO KRC-20 rebate — not applicable to ZKas** (rebate BPS are zeroed). |
| [`api/`](api/) | Read-only `axum` HTTP API. Serves **aggregate** pool stats only; per-miner stats are withheld for miner privacy. |
| [`katpool/`](katpool/) | Main wiring binary that runs the active components in one process. |
| [`crates/`](crates/) | Shared libraries: `katpool-domain`, `-db`, `-config`, `-metrics`, `-storagemass`, `-idempotency`, `-telemetry`, `-secrets`, `-fault-injection`. |

## Payout model (ZKas)

**Today (live): direct payout.** The pool runs without a coinbase override, so every
block template pays the coinbase **directly to the address the finding miner mined
with**. Rewards are minted as shielded notes to the miner's own `zkas:` address by the
chain itself — the pool is never in custody and there is nothing to claim.

**Custodial mode (in development):** the pool mines to its **own shielded (`$zkas`)
treasury address** and tracks each miner's PROP balance in Postgres, keyed by the
miner's payout shielded address. A scheduled sweep then pays each miner's **full
accrued balance** (above a dust threshold) to their `zkas:` address automatically —
**no signature, no claim, no vesting, nothing for the miner to do.** No mining
password is required.

## Connecting a miner

ZKas mining is ordinary kHeavyHash — point any Kaspa-style miner/ASIC at the pool's
stratum port with your `zkas:` payout address (legacy `firecash:` accepted) as the username:

```
stratum+tcp://<pool-host>:<port>   user=zkas:<your-address>   pass=x
```

(The password field is unused; `x` or empty is fine.) See [`help.txt`](help.txt) for the
full operator guide, native vs. AuxPoW merged mining, and node setup.

## Quick start (development)

Prerequisites: Rust 1.91+ / edition 2024 (pinned via `rust-version` in
[`Cargo.toml`](Cargo.toml)), Docker (for ephemeral test databases), and
`cargo-deny` / `cargo-audit`.

The patched `kaspa-*` / ZKas crates are pulled straight from the
[`firecash-rusty`](https://github.com/firecash/firecash-rusty) fork over git — you do
**not** need a local rusty-kaspa checkout at any particular path (Cargo fetches and pins it).

```bash
git clone https://github.com/firecash/firecash-pool.git
cd firecash-pool

# Verify your environment matches CI gates
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo deny check

# Build / run the wiring binary
cargo run --release --bin katpool
```

You also need a reachable ZKas node (`kaspad`) with `--utxoindex`. See
[`help.txt`](help.txt) §3 for node setup and the stratum bridge config
([`firecash-bridge.yaml`](firecash-bridge.yaml)).

## Operating principles

- **Determinism**: every reward, mass, and payout calculation is a pure function tested
  with `proptest`. No floating-point money math.
- **Zero plaintext secrets**: the treasury key only ever exists encrypted on disk;
  loaded via systemd `LoadCredentialEncrypted`, mlocked, zeroized on drop.
- **Idempotent payouts**: every outbound transaction records an idempotency key in the DB
  *before* signing. Mid-cycle restarts cannot double-pay.
- **Miner privacy**: the pool does not publish per-miner or top-miner leaderboards; only
  aggregate stats are exposed.
- **Pinned everything**: Rust toolchain, container images, and crate versions are pinned.

## Documentation

Start at [`docs/README.md`](docs/README.md) for the index and [`help.txt`](help.txt) for
the operator guide. Some `docs/` pages still carry upstream (Kaspa/KRC-20) specifics and
are being migrated to ZKas.

## License

Dual-licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT license](LICENSE-MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual-licensed as above, without any additional terms or conditions.
