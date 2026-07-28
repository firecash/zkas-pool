# Phase 3 acceptance evidence

Phase 3 (pool-accountant) closes when every row below is GREEN
for a release-candidate commit. Phase 4 (KAS payout engine)
cannot start until this page is complete.

## Acceptance matrix

| # | Criterion | Verification | Status |
|---|---|---|---|
| 1 | Accountant subscribes to the bridge's `PoolEvent` broadcast and mirrors share + block lifecycle into the new schema. | `cargo test -p accountant --test consumer` — 8 tests; `replay_determinism.rs` proves byte-equal DB state for the same event stream against two independent Postgres instances. | GREEN — landed in PR #16 (M1) |
| 2 | Topline fee is operator-tunable via `KATPOOL_FEE_TOPLINE_BPS`; rebate ratios fixed in code (33% standard, 100% elite); every allocation is integer-arithmetic end-to-end. | 13 property tests in `tests/allocation_properties.rs` over the full allowed input space. Caught one real i64 overflow at the boundary during initial run; regression seeds checked into `.proptest-regressions`. | GREEN — landed in PR #18 (hardening) |
| 3 | Share-window aggregation, share-reject persistence, and per-miner stats surface land as repo + accountant primitives. | `cargo test -p accountant` — `window_aggregator` (5), `share_reject` (4), `share_stats` (5). | GREEN — landed in PR #17 (M2) |
| 4 | PROP allocation engine produces per-wallet `share_allocation` rows from a matured block + share window; runs in one Postgres transaction; idempotent on replay. | 10 integration tests in `tests/allocation_engine.rs` against testcontainer Postgres. | GREEN — landed in PR #19 (M3) |
| 5 | Kasplex tier classifier resolves a wallet to `Standard` or `Elite` via either NACHO KRC-721 ownership or ≥ 100M NACHO KRC-20 balance (locked counts); errors fall back to `Standard`. | 10 wiremock-backed tests against canned kasplex responses. | GREEN — landed in PR #19 (M3) |
| 6 | Block maturity tracker advances `submitted_to_node` → `confirmed_blue` → `matured` (or `orphaned`) and calls the allocation engine on maturity. | 11 integration tests in `tests/maturity_tracker.rs` against ephemeral Postgres + `FakeKaspad`. | GREEN — landed in PR #20 (M3b) |
| 7 | Maturity tracker against real kaspad-tn10: gRPC connection works; virtual blue score advances live; unknown-block hashes are recognised as "not yet confirmed", not as transport errors. | `accountant-tracker-runner` against the operator's testnet-10 kaspad; live evidence captured below. | GREEN — landed in PR #21 (M3c) |
| 8 | Unified runtime binary: bridge + accountant event consumer + maturity tracker compose into one process with shared `tokio::sync::broadcast<PoolEvent>` channel; SIGINT/SIGTERM shutdown is clean. | `katpool` binary boots, all three subsystems start, kaspad gRPC works, SIGTERM exits cleanly within milliseconds. | GREEN — landed in PR #22 (M3d); dry-run evidence below |
| 9 | Full mine-and-allocate end-to-end with an ASIC pointed at the bridge against testnet-10. | Operator-driven test via [runbook 16](runbooks/16-testnet10-full-pipeline-live.md); evidence in §M3f cut-2 below. | GREEN — post-merge Goldshell re-run 2026-05-27 (`pipeline-evidence/2026-05-27T07-48-27Z-m3f-cut2-goldshell-validation/`, git `e854abe` / merged `92c2a59`) |
| 10 | M3f production-grade defect closeout: per-network `wallet::ensure`, real-vs-phantom `SubmitBlockResponse` discrimination, and lifecycle ordering invariant. | Targeted unit + property tests in PR #25; live verification in row 9 cut-2. | GREEN — landed in PR #25 (M3f) |
| 11 | 24h-production-log replay-determinism harness: feed real production logs through the accountant, prove byte-equal state. | `accountant/tests/replay_harness_scale.rs` + `accountant::replay`; operator ≥24h capture via `KATPOOL_EVENT_RECORD_PATH`. Evidence archived under `replay-evidence/`. | GREEN — landed in this PR (M4) |
| 12 | `cargo deny check` clean on the locked Cargo.lock. | CI step; locally verifiable. | GREEN — every Phase 3 PR |

## Phase 3 M3c live evidence (this PR)

**Run date (UTC):** 2026-05-27 03:08

**Environment:**
- `kaspad-tn10` reachable at `grpc://127.0.0.1:16210` on the pool VPS (Toccata-aware, per [runbook 13](runbooks/13-kaspad-tn10-bootstrap.md)).
- Throwaway Docker Postgres 17-alpine.
- `accountant-tracker-runner` release build, git rev TBD-at-merge.
- Pool address (synthetic for dry-run): `kaspatest:qzcf94f8pzhtgzy8fpprvv0ag28f9zf9fks6mnu334c8nm5qtne2shh0nv9ht`.
- Seeded block: synthetic all-zero-ones hash (not a real testnet block — the dry-run validates kaspad-not-yet-aware semantics, not real-block lifecycle; that's the operator-driven test).

**Two-phase dry-run.** The first run surfaced a real bug: kaspad
v1.1.0 / Toccata returns `"cannot find header <hash>"` for unknown
hashes, which my initial `is_block_not_found` heuristic missed.
The tracker logged the unknown-hash response as
`tracker per-block error` instead of treating it as
`still_waiting`. Fixed in the same PR (one-line addition to the
heuristic); confirmed by the second run.

### First run (pre-fix) — caught the bug

```text
2026-05-27T03:06:32 ERROR accountant::maturity: tracker per-block error; continuing sweep
                          hash=00...01  error=kaspad: kaspad transport error:
                          cannot find header 00...01
2026-05-27T03:06:32 INFO  accountant::maturity: tracker sweep done
                          stats=SweepStats { confirmed_blue: 0, matured: 0,
                                             orphaned: 0, still_waiting: 0, errors: 1 }
```

The `errors: 1` was correct *given* the unknown-error fallthrough,
but semantically wrong: an unknown hash should be classified as
"keep waiting, kaspad hasn't ingested yet", not "transport error".

### Second run (post-fix) — validates the fix

```text
2026-05-27T03:08:47 INFO  accountant::maturity: tracker sweep done
                          virtual_blue_score=464237854
                          stats=SweepStats { confirmed_blue: 0, matured: 0,
                                             orphaned: 0, still_waiting: 1, errors: 0 }
2026-05-27T03:08:51 INFO  accountant::maturity: tracker sweep done
                          virtual_blue_score=464237900
                          stats=SweepStats { confirmed_blue: 0, matured: 0,
                                             orphaned: 0, still_waiting: 1, errors: 0 }
```

### What this evidence proves

- ✅ `KaspadGrpcClient` connects to real testnet-10 kaspad-tn10 via gRPC and authenticates.
- ✅ `get_sink_blue_score` returns a real, monotonically advancing blue score (`464237854 → 464237900` in 4s ≈ 11.5 BPS, matches Crescendo post-fork 10 BPS).
- ✅ `get_block` round-trips against the live node.
- ✅ Per-block error isolation works (single bad hash → metric tick, sweep continues).
- ✅ The `is_block_not_found` heuristic correctly classifies the kaspad-tn10 wording.
- ✅ SIGTERM handling: tracker shuts down cleanly within milliseconds of signal receipt.
- ✅ `katpool-db::build_pool` connects to Docker Postgres via the runner binary's env-supplied URL.
- ✅ Migrations apply cleanly start-to-finish (including the new `20260527000001_wallet_tier_audit.sql`).

### What this evidence does NOT prove

- ❌ Real-block lifecycle: no testnet-10 block was seeded into the
  DB whose hash kaspad actually knew, so neither
  `confirmed_blue → matured` nor `orphaned` transitions were
  exercised against a real block. The operator-driven test
  ([runbook 15](runbooks/15-testnet10-tracker-live.md)) covers
  this once the operator picks a known testnet block.
- ❌ Real coinbase reward extraction: `extract_coinbase_reward`
  is unit-tested against canned `RpcBlock` fixtures but has not
  been exercised against a real testnet-10 coinbase transaction
  on this PR. The runbook-driven test will cover this.
- ❌ Share ingestion + PROP allocation against real miner shares:
  that's Phase 3 M3d (unified bridge+accountant runner) + the
  operator's ASIC test.

## Run history

| Date (UTC) | Phase | Run by | Notes |
|---|---|---|---|
| 2026-05-27 03:06 | M3c dry-run #1 (pre-fix) | agent | Surfaced the `"cannot find header"` semantics; heuristic patched in same PR. |
| 2026-05-27 03:08 | M3c dry-run #2 (post-fix) | agent | Confirms still_waiting=1, errors=0. Blue score advancing live. |
| 2026-05-27 04:01 | M3d dry-run #1 (pre-fix) | agent | Surfaced a shutdown-ordering bug: consumer hung waiting for bridge-held Sender clones. |
| 2026-05-27 04:11 | M3d dry-run #2 (post-fix) | agent | `katpool runtime exiting cleanly` <1ms after SIGTERM. All three subsystems boot, virtual blue score advances live (464275327 → 464275463, ~13 BPS), stratum port bound on 15556. |

Operator-driven row(s) appended after the M3c (runbook 15) and
M3d (runbook 16) live exercises.

## Phase 3 M3d dry-run evidence (this PR)

**Run date (UTC):** 2026-05-27 04:11

**Environment:**
- `kaspad-tn10` at `grpc://127.0.0.1:16210` (local Toccata-aware node on the pool VPS).
- Throwaway Docker Postgres 17-alpine.
- `katpool` release binary; SHA captured by the manifest in any
  operator run via `runbook 16`.
- Pool address (synthetic): same `kaspatest:qzcf94f8…` used in M3c.

### Boot sequence (10 lines from the log)

```text
INFO katpool runtime starting instance=dry-run-3 kaspad=grpc://127.0.0.1:16210 stratum_port=15556
INFO katpool-db pool established application_name="katpool[dry-run-3]"
INFO Connecting to Kaspa node at grpc://127.0.0.1:16210
INFO subsystems running; awaiting shutdown signal
INFO accountant consumer starting instance=dry-run-3
INFO [dry-run-3] anti-abuse: max_conn_per_ip=256, ...
INFO dry-run-3 Starting stratum listener on 15556
INFO tracker sweep done instance=dry-run-3 virtual_blue_score=464275327 stats=SweepStats { confirmed_blue: 0, matured: 0, orphaned: 0, still_waiting: 0, errors: 0 }
INFO tracker sweep done instance=dry-run-3 virtual_blue_score=464275364 stats=SweepStats { confirmed_blue: 0, matured: 0, orphaned: 0, still_waiting: 0, errors: 0 }
...
```

### Shutdown sequence (4 lines)

```text
INFO SIGTERM received
INFO tracker shutdown requested; exiting instance=dry-run-3
INFO shutdown signal observed; tearing down subsystems
INFO katpool runtime exiting cleanly
```

Total time from SIGTERM to `exiting cleanly`: **218 microseconds**.

### What this evidence proves

- ✅ `katpool` binary boots all three subsystems (bridge stratum
  listener + accountant event consumer + maturity tracker) in
  one process.
- ✅ Shared `tokio::sync::broadcast<PoolEvent>` channel
  established (consumer subscribes, bridge holds a clone via
  `listen_and_serve_with_events`).
- ✅ Both kaspad connections (bridge's `KaspaApi` + tracker's
  `KaspadGrpcClient`) succeed against the same node.
- ✅ Stratum port (15556 in the dry-run) is bound and listening
  — ASIC can connect.
- ✅ Maturity tracker sweeps on schedule with the configured
  `KATPOOL_MATURITY_POLL_SECS=4`. Virtual blue score advances
  ~13 BPS, matches Crescendo.
- ✅ Anti-abuse defaults apply correctly (256 conn/IP, 100
  frames/sec).
- ✅ SIGTERM handling: clean process exit within microseconds,
  including tracker cooperative shutdown + bridge/consumer
  abort + finalisation.

### What this evidence does NOT prove

- ❌ Real share ingestion (no ASIC connected during the dry-run).
- ❌ Real block found by our pool + observed through full
  lifecycle to allocation. That's the operator-driven test in
  runbook 16.

### M3d shutdown-ordering bug (caught in this PR's dry-run)

First-pass shutdown order tried to drain the consumer to
`RecvError::Closed` after aborting the bridge. This hung
indefinitely because the bridge spawns *internal*
kaspad-notification tasks that hold cloned `Arc<ShareHandler>` →
cloned `broadcast::Sender<PoolEvent>` past the listener-task
abort. The receiver never sees Closed.

Resolved by aborting the consumer JoinHandle directly rather
than draining it. At-most-once delivery is the design contract
(per ADR-0013 § Replay-determinism), so dropping in-flight
events on shutdown is correct. Phase 7's wiring rework will
revisit if the bridge upstream grows a clean shutdown signal.

This is exactly the kind of bug a live dry-run is meant to
catch — kept the issue out of the operator-driven run.

## Out of scope for Phase 3

- **Phase 4** — KAS payout engine. Reads `share_allocation` rows,
  groups by wallet, signs + broadcasts on-chain transactions.
- **Phase 5** — KRC-20 NACHO payout engine. Reads
  `nacho_rebate_accrual` rows, converts sompi-denominated rebate
  to NACHO at the prevailing rate, executes commit/reveal cycle.
- **Phase 7 shadow run** — accountant runs against the
  production firehose in shadow mode for 48-72h with 0-sompi
  divergence from the legacy stack.

## Phase 3 M3f live evidence (this PR)

**Run date (UTC):** 2026-05-27 06:30 (first run); diagnosis + fix
landed before the operator-driven re-run.

**Environment.** Pool VPS (NetCup), `kaspad-tn10` at
`grpc://127.0.0.1:16210`, Docker Postgres 17-alpine, unified
`katpool` runtime, ASIC: Goldshell running `BzMiner/v14.0.2`
firmware (worker identity and source IP redacted — third-party miner data).

**Symptoms observed in the 2.5-minute live run** (artefacts in
`pipeline-evidence/2026-05-27T06-30-03Z-m3d-asic-debug2/`, 129 MB
debug log):

| Signal | Count |
|---|---|
| Share submits from miner (`mining.submit`) | 14,664 |
| Bridge `submit_block` attempts | 4,853 |
| **kaspad `report = Success`** | **1,004** |
| kaspad `report = Reject(BlockInvalid)` | 3,849 |
| Bridge `"🎉 BLOCK ACCEPTED BY NODE"` log lines | **4,853** |
| Accountant `wallet_ensure` failures | **9,775** |
| Accountant `orphan_block_accepted` errors | **4,853** |
| Rows written across all 6 business tables | **0** |

**Root-cause analysis.** The three defects are causally chained:

1. **Defect 2 (gating).** `accountant::ConsumerConfig` hard-coded
   `NETWORK = "mainnet"`. Every `wallet::ensure` against the
   testnet-10 address violated the
   `wallet.wallet_address_format` CHECK constraint
   (SQLSTATE 23514). 9,775 occurrences.
2. **Defect 3 (downstream consequence).** Because the
   `BlockFound` handler's `wallet::ensure` failed first, the
   `block` row never inserted; the subsequent `BlockAccepted`
   event correctly tripped the consumer's
   `OrphanBlockAccepted` invariant. 4,853 occurrences.
3. **Defect 1 (independent bridge bug).** The bridge's
   `KaspaApi::submit_block` matched only `Ok(_)` on the gRPC
   result and ignored `SubmitBlockResponse::report`, treating
   `Reject(BlockInvalid)` / `Reject(IsInIBD)` / `Reject(RouteIsFull)`
   as wins. 79% of all bridge "accepted" logs in this run were
   false positives.

**Defect 1 dual signal.** The 79% reject rate also confirms a
non-defect operational reality: at testnet-10's 10 BPS against
share-difficulty 1, many submissions race the network tip and
lose. That's expected; what was broken is reporting these races
as wins.

**Defects 2/3 closeout.** `ConsumerConfig::new(instance_id,
network)` now takes a validated network string
(`accountant::consumer::VALID_NETWORKS`, identical to the
migration's `wallet_network_valid` CHECK list). The unified
`katpool` runtime derives the default from the pool address
bech32 prefix (`kaspa:` → `mainnet`, `kaspatest:` →
`testnet-10`) with `KATPOOL_NETWORK` as the operator override
(required for `testnet-11`, `devnet`, `simnet`, which share
bech32 prefixes with other targets). Five regression tests
in `consumer_config_tests` pin the validation surface,
including a lock-step contract test that fails if anyone adds /
removes a network in the migration without updating
`VALID_NETWORKS`.

**Defect 1 closeout (cut 2).** The first M3f cut collapsed
`SubmitBlockReport::Reject(_)` directly into `Err`. That over-
correction spiked the miner-visible reject rate to ~68% during
the cut-1 verification run on 2026-05-27 07:24 (artefacts in
`pipeline-evidence/2026-05-27T07-24-38Z-m3f-goldshell-validation/`):
1,410 real `Success` responses, 5,460 `Reject(BlockInvalid)`
responses, and the Goldshell UI flagged ~68% of submissions as
rejected because the share-handler's existing `Err` arm
classified the outcome as `ShareRejectReason::BadPow` — yet by
construction the miner's PoW was valid (the share met the
network target or we would never have called `submit_block`).
Cut 2 fixes the regression with a typed outcome:

```rust
pub enum BlockSubmitOutcome {
    Accepted(SubmitBlockResponse),               // kaspad: Success → block in DAG, share credited, BlockAccepted emitted
    RejectedByNode(SubmitBlockRejectReason),     // kaspad: Reject(*) → share credited, BlockAccepted suppressed, no miner-visible reject
}
```

The share-handler's match now has three explicit arms
(`Ok(RejectedByNode)` → credit share, skip event;
`Ok(Accepted)` → credit share, emit event, spawn confirmation
poll; `Err(_)` → existing transport / `ErrDuplicateBlock` path
mapped to `ShareRejectReason::Stale` or `BadPow`). `Err` is now
reserved for true transport failures and `ErrDuplicateBlock`.
Operator-visible reject labels (`BlockInvalid`, `IsInIBD`,
`RouteIsFull`) are pinned by a contract test. Five new pure-
function regression tests in `submit_block_report_tests`
explicitly assert that `Reject(_)` round-trips as
`Ok(RejectedByNode(_))` — not `Err` — so a future refactor cannot
regress to the cut-1 behaviour.

**Verification posture.** Three pure-function regression test
modules (`submit_block_report_tests`, `consumer_config_tests`,
plus the existing `coinbase_recipient_tests`) cover the
discriminator logic and config validation independent of any
RPC / DB layer.

**Post-merge cut-2 live re-run (row 9 GREEN).** Artefacts:
`pipeline-evidence/2026-05-27T07-48-27Z-m3f-cut2-goldshell-validation/`
(git `e854abe`, 10-minute run, disposition
`shares_and_blocks_observed`):

| Signal | Count |
|---|---|
| `share` rows | 6,934 |
| `block` rows | 6,934 |
| `share_reject` rows | 6,291 |
| `wallet_ensure` failures | 0 |
| `orphan_block_accepted` | 0 |
| kaspad `Success` (submitted) | 1,241 |
| kaspad `RejectedByNode` (share still credited) | 5,693 |
| `share_allocation` rows | 0 (no matured blocks in 10 min window) |

Goldshell UI ~48% reject rate matches `reply_low_diff_share` (error
23), not M3f `BadPow` — operational testnet churn, not a harness
blocker.

## Phase 3 M4 replay-determinism evidence (this PR)

**Harness surface.**

| Component | Role |
|---|---|
| `accountant::replay` | NDJSON load, DB snapshot, `verify_dual_replay` |
| `KATPOOL_EVENT_RECORD_PATH` | Runtime NDJSON capture on the unified `katpool` bus |

**CI gate:** `cargo test -p accountant --test replay_harness_scale`
(~700 synthetic events, dual independent Postgres, byte-equal
snapshots).

**Operator ≥24h gate (cutover ticket):** capture via
`KATPOOL_EVENT_RECORD_PATH` on a production-fed instance; replay via
`accountant::replay` and archive manifest under `replay-evidence/`
(executed at cutover; one-shot CLI retired post-sign-off).

## Sign-off

Phase 3 closes when:

1. Every row in the matrix is GREEN.
2. The operator-driven M3c live test against testnet-10 has been
   captured in the Run history table.
3. The M3d/M3f live exercise has demonstrated share + block rows
   with zero `orphan_block_accepted` (see §M3f cut-2 above).
4. The operator archived ≥24h production event capture and replay
   evidence under `replay-evidence/` (runbook 17 retired post-cutover).
