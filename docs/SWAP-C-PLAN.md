# Plan C — ZKAS omnichain bridge (ZKAS on Kaspa ⇄ shielded ZKAS on the ZKas chain)

Status: DRAFT / grounded in a code audit of `rusty-kaspa` (ZKas fork) + `zkas-pool`.
Supersedes the earlier "atomic swap" framing (kept as §11 for the record — it was invalidated
by the all-shielded finding in §1.2).

**Goal:** ZKAS exists as the *same asset on two networks* — a transparent, tradeable token on
**Kaspa** and the shielded native coin on the **ZKas chain** — connected by a **1:1 bridge**.
Users buy ZKAS on Kaspa (easy, liquid) and move it to the ZKas chain to use privacy.

**Why omnichain beats a swap:** the bridge moves *the same asset 1:1*, so there is **no price
risk and no market-maker**. Price discovery happens on a Kaspa DEX; privacy happens on ZKas.

---

## 1. Code audit — the constraints that shape everything

### 1.1 Kaspa side — full HTLC / script is native
`crypto/txscript/src/opcodes/mod.rs` (our fork) has: `OpCheckLockTimeVerify` (0xb0, active),
`OpCheckSequenceVerify`, `OpSHA256`/`OpBlake2b`/`OpBlake3`, `OpIf/OpElse/OpEndIf`, `OpEqual`,
`OpCheckSig` (Schnorr secp256k1, x-only via `OpData32`), `OpCheckSigFromStack`. Toccata extras
already present: `OpChainblockSeqCommit`, `OpTxInputSeq`, `OpTxLockTime`, and a `covenant` field
on `TransactionOutput`. ⇒ hashlock/timelock scripts and covenant-governed logic are expressible.

### 1.2 ZKas side — ALL-SHIELDED (the defining constraint)
`shielded-core/src/turnstile.rs` (hard consensus rule from commit one):
> "In an all-shielded chain there is no transparent ledger …
>  total_value_in_shielded_pool == cumulative_coinbase_issued − cumulative_fees_paid"

`consensus/.../tx_validation_in_isolation.rs`: shielded txs MUST have no transparent
inputs/outputs (lines 43/46); value enters the pool **only via coinbase** ("the one transparent
seam"); fees are the only exit.

⇒ **No user-held transparent ZKAS UTXOs exist.** Every ZKAS is an Orchard note; notes have **no
script** → cannot be hashlocked, timelocked, or covenant-governed natively. Any bridge/reserve
logic on the ZKas side must live in **consensus rules**, not in scripts or an escrow UTXO.

### 1.3 Tooling we can reuse
- `submit_transaction` RPC on both nodes (`rpc/core/src/api/rpc.rs:238`).
- `shielded-pay` CLI (`new/address/info/mempool/balance/send/sign`) — ZKas-side delivery.
- **`payout-krc20`** crate (`engine.rs`): deploy / mint / transfer KRC-20 — Kaspa-side token, today.
- `wallet/core` tx generator + `crypto/txscript` `ScriptBuilder` — Kaspa tx construction.
- In-consensus Kaspa verifier (inherited — ZKas is a Kaspa fork).
- Pool infra on VPS1 (already talks to ZKas node :16110 and Kaspa node :16215).
- Existing inventory: pool's mined ZKAS (`firecash:pyfjy…`) + KAS treasury (`kaspa:qz9vu847…`).

---

## 2. Architecture

```
        KASPA network                    │  BRIDGE (1:1)  │        ZKAS network
   ─────────────────────────             │                │   ─────────────────────
   ZKAS as a token                       │  burn ⇄ mint   │   ZKAS native, SHIELDED
   • transparent, tradeable    ──────────┼──── peg-in ───▶│   • private (Orchard)
   • KAS/ZKAS pool on a DEX              │◀─── peg-out ───┤   • mined coinbase = issuance
   • buy like any Kaspa token            │                │   • transact privately
```

- **Canonical issuance = ZKas mining** (60 ZKAS/block, 1 BPS). The Kaspa-side token is a
  **1:1 mirror**, backed by ZKAS removed from ZKas circulation during peg-out.
- **Non-inflationary:** Kaspa-side supply always equals ZKAS locked/burned into the bridge.

### User journey (the primary flow = peg-in)
1. **Buy:** user swaps KAS → ZKAS on a **Kaspa DEX** (market price, deep liquidity, no pool involvement).
2. **Transfer = peg-in:** user burns/sends ZKAS-token to the bridge on Kaspa, payload carrying
   their shielded destination address → bridge delivers **shielded ZKAS** on the ZKas chain, 1:1.
3. **Use privately** on the ZKas chain.
4. **Peg-out** (reverse) to trade/exit back on Kaspa.

---

## 3. Trust model — the asymmetry (this is the heart of it)

| Direction | Who verifies whom | Trustless cost |
|---|---|---|
| **Peg-in** (Kaspa → ZKas) — the user's main flow | **ZKas verifies Kaspa** (built-in) | 🟢 Cheap. Consensus mint rule. **No admin key.** |
| **Peg-out** (ZKas → Kaspa) | **Kaspa verifies ZKas** | 🔴 Hard. Needs **KIP-16** Groth16 exit proof (months). |

**Why:** ZKas is a Kaspa fork → it already contains a full Kaspa verifier, so "a burn happened on
Kaspa" is a native consensus check. Kaspa cannot read our shielded chain → the only trustless
proof of a ZKas burn is a KIP-16 proof of ZKas state R (the Arch-A exit circuit; tractable because
merged mining already makes Kaspa witness R, so kHeavyHash is never proven in-circuit).

**Consequence:** the direction users care about most (buy on Kaspa → shield on ZKas) is the
trustless-cheap one. Only the exit needs the hard circuit.

---

## 4. Two design decisions required for trustlessness

The custodial v1 shape does NOT upgrade cleanly. If trustlessness is the goal, commit to these
from the start:

1. **Mint/burn, not a guarded reserve.** A reserve on a shielded chain can't be covenant-governed
   (notes have no scripts) → it needs an admin key. Instead: peg-in **mints** shielded ZKAS
   (new turnstile seam), peg-out **burns** it. No reserve to guard = no key to guard it.
2. **Native covenant token (Toccata KIP-20), not KRC-20.** For ZKas to verify a Kaspa-side burn
   *in consensus*, the token state must live in Kaspa consensus. KRC-20 is an overlay/indexer
   standard → verifying it in-consensus means embedding a KRC-20 indexer in the ZKas node
   (fragile). A native covenant token makes a burn a real, Merkle-provable consensus transition.
   *Use KRC-20 only for a throwaway custodial demo; native for the real version.*

---

## 5. Trustlessness roadmap

| Phase | Peg-in | Peg-out | Admin key? |
|---|---|---|---|
| **v0 custodial** (fast demo) | operator mints on ZKas | operator mints KRC-20 on Kaspa | Yes (single) |
| **v1 trustless peg-in** | **ZKas consensus** verifies Kaspa burn → mints shielded ZKAS | M-of-N federation covenant | **None on peg-in;** federation on peg-out |
| **v2 fully trustless** | consensus rule | **KIP-16** covenant releases against a ZKas-burn proof | **None** |

- Pool **seeds Kaspa liquidity with its own ZKAS** (a peg-out of *pool funds* — custodial but no
  user trust, it's our money), so a KAS/ZKAS market exists for users to peg-in against.
- Federation covenant removes the single key and upgrades in place to the KIP-16 rule.

---

## 6. Components

| # | Component | Phase | Reuses | Est. |
|---|---|---|---|---|
| 1 | Kaspa-side ZKAS token — deploy/mint/burn | v0: KRC-20; v1+: native covenant token | `payout-krc20` / covenant opcodes | v0 S / v1 L |
| 2 | Bridge service — watch both chains, state machine + sqlite, VPS1 | v0 | pool infra, `shielded-pay`, kaspa RPC | L |
| 3 | Peg-in consensus rule — verify Kaspa burn (Merkle proof) + mint shielded ZKAS (turnstile seam) | v1 | in-consensus Kaspa verifier, turnstile | L |
| 4 | Peg-out federation covenant (M-of-N) | v1 | covenant opcodes | M |
| 5 | Peg-out KIP-16 exit circuit + covenant release on Kaspa | v2 | merged-mining witness, Arch-A circuit | XL |
| 6 | DEX liquidity seeding (KAS/ZKAS) | v0 | inventory | M |
| 7 | Proof-of-reserves / supply dashboard (locked-vs-minted; reserve viewing key if custodial) | v0 | shielded-pay balance | S |

---

## 7. Pricing (answers "1:1 or pool?")

- **The bridge is 1:1** — same asset, no price, no market-maker, no spread.
- **Price discovery is external** — the KAS/ZKAS market on a Kaspa DEX. We don't set it.
- 1:1 works here *because it's the same coin*, unlike a KAS↔ZKAS swap (two different coins) where
  1:1 is impossible and price must float.

---

## 8. Risks

- **Peg-out trust (v0/v1):** custodial → federation → KIP-16. Communicate the current tier honestly.
- **Consensus changes (v1 mint seam, v2 exit):** live-chain upgrades; need testnet + activation gate
  (reuse the merged-mining-activation pattern, task #40). Turnstile invariant must be extended
  carefully — the bridge mint is a *new authorized seam*, not a hole.
- **Native token on Kaspa (v1):** depends on Toccata covenant-token availability on mainnet; KRC-20
  is the fallback for custodial demos only.
- **Proof-of-reserves on a shielded chain:** backing isn't publicly visible; publish the reserve
  viewing key (custodial tiers) or rely on the consensus mint/burn accounting (trustless tiers).
- **KIP-16 curve unknown:** BN254 vs BLS12-381 gates the exit-circuit design (shared with Arch A).

---

## 9. Build order

1. **v0 (weeks):** deploy KRC-20 ZKAS on Kaspa (`payout-krc20`); bridge service (watch → mint via
   `shielded-pay`, and reverse); seed a KAS/ZKAS DEX pool with pool funds; PoR dashboard. Custodial,
   honestly labeled. Proves the product + seeds liquidity.
2. **v1 (months):** native covenant token on Kaspa; **trustless peg-in** consensus rule on ZKas
   (verify Kaspa burn → mint, turnstile seam); federation covenant for peg-out. Removes the admin
   key on the user's main flow.
3. **v2:** KIP-16 exit circuit + covenant release → fully trustless peg-out. Shares the Arch-A keystone.

Parallel/non-blocking: pin the **KIP-16 curve** from kaspanet source (gates v2 and Arch A).

---

## 10. First concrete task

Depends on the trust bar chosen:
- **If shipping fast (custodial demo first):** deploy the KRC-20 ZKAS token via `payout-krc20`,
  then the bridge watcher (Kaspa token→bridge triggers `shielded-pay send`).
- **If trustless-first (recommended per §4):** design the **native covenant token** + the
  **ZKas peg-in consensus rule** (Kaspa-burn Merkle verification + turnstile mint seam) — this is
  the version that is actually trustless on the path users travel, and it does not throw away work.

---

## 11. Superseded: the atomic-swap framing (for the record)

Earlier plan treated KAS↔ZKAS as an HTLC atomic swap. Invalidated by §1.2: ZKas has no transparent
UTXOs, so its leg can't be hashlocked; trustless swap would require a hash-locked *shielded note*
(consensus change) or cross-curve RedPallas↔secp256k1 adaptor signatures (novel crypto). The
omnichain 1:1 bridge (this doc) is strictly better for the stated goal ("ZKAS on both networks,
buy on Kaspa, move to ZKas") because it avoids price risk and market-making entirely.
```
Key correction vs. earlier: a KAS↔ZKAS swap trades two DIFFERENT coins (price floats, MM needed);
the omnichain bridge moves ONE coin across two networks (1:1, no price). Different problems.
```
