//! Payout aggregates — cycle, individual payout, and the KRC-20
//! commit/reveal state machine.
//!
//! All three tables share a strict idempotency story: each cycle has
//! a human-readable `idempotency_key` (`kas-<daa_start>-<daa_end>` or
//! `krc20-<daa_start>-<daa_end>`) and each payout has the
//! `UNIQUE (cycle_id, wallet_id)` guard. Retrying a broadcast that
//! partially succeeded is `INSERT ON CONFLICT DO NOTHING` for the
//! cycle and a no-op for already-existing payout rows.

// daa_start/daa_end columns are BIGINT-bounded by chain reality.
#![allow(clippy::cast_possible_wrap)]

use chrono::{DateTime, Utc};
use katpool_domain::{BlockHash, DaaScore};
use sqlx::PgExecutor;

use crate::DbError;
use crate::repo::WalletId;

// ---- enums ----------------------------------------------------------

/// Kind of payout cycle. Mirrors the `payout_kind` Postgres enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "payout_kind", rename_all = "snake_case")]
pub enum PayoutKind {
    /// KAS payout cycle (native KAS transactions).
    Kas,
    /// KRC-20 NACHO payout cycle (commit/reveal pair per recipient).
    Krc20Nacho,
    /// ZKas shielded payout cycle (one Orchard shielded transaction per
    /// recipient, sent from the pool's shielded treasury).
    Zkas,
}

/// Cycle status state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "payout_cycle_status", rename_all = "snake_case")]
pub enum PayoutCycleStatus {
    /// Allocations computed; transactions not yet broadcast.
    Planned,
    /// Transactions in flight on the wire.
    Broadcasting,
    /// Some recipients confirmed, others still pending.
    PartiallySettled,
    /// Every recipient confirmed on chain.
    Settled,
    /// Broadcast errored; needs investigation.
    Failed,
}

impl PayoutCycleStatus {
    /// Stable snake-case label (matches the `payout_cycle_status` SQL enum),
    /// suitable for a low-cardinality Prometheus `status` label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Broadcasting => "broadcasting",
            Self::PartiallySettled => "partially_settled",
            Self::Settled => "settled",
            Self::Failed => "failed",
        }
    }

    /// Whether the cycle made forward progress on chain (at least one recipient
    /// confirmed). Used to stamp the payout last-success metric.
    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Settled | Self::PartiallySettled)
    }
}

/// Per-recipient payout status state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "payout_status", rename_all = "snake_case")]
pub enum PayoutStatus {
    /// Cycle allocation produced this row; no tx yet.
    Planned,
    /// Transaction submitted to mempool.
    Submitted,
    /// Transaction accepted by network (first confirmation).
    Accepted,
    /// Confirmed past maturity window.
    Confirmed,
    /// Transaction failed; `failure_reason` carries detail.
    Failed,
}

/// KRC-20 transfer status state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "krc20_transfer_status", rename_all = "snake_case")]
pub enum Krc20TransferStatus {
    /// Commit transaction not yet submitted.
    Pending,
    /// Commit tx submitted (on chain).
    CommitSubmitted,
    /// Reveal tx submitted (on chain).
    RevealSubmitted,
    /// Both commit and reveal confirmed.
    Completed,
    /// Transfer failed irrecoverably.
    Failed,
}

// ---- rows -----------------------------------------------------------

/// One row of `payout_cycle`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PayoutCycle {
    /// Synthetic primary key.
    pub id: i64,
    /// Cycle kind.
    pub kind: PayoutKind,
    /// Status state.
    pub status: PayoutCycleStatus,
    /// Half-open DAA range start (inclusive).
    pub daa_start: i64,
    /// Half-open DAA range end (exclusive).
    pub daa_end: i64,
    /// When the cycle row was created.
    pub planned_at: DateTime<Utc>,
    /// When the broadcast started.
    pub broadcast_at: Option<DateTime<Utc>>,
    /// When the last recipient confirmed.
    pub settled_at: Option<DateTime<Utc>>,
    /// Sum of payout amounts across all recipients in the cycle.
    pub total_sompi: i64,
    /// Number of recipients in the cycle.
    pub total_recipients: i32,
    /// Human-readable idempotency key.
    pub idempotency_key: String,
}

/// One row of `payout`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Payout {
    /// Synthetic primary key.
    pub id: i64,
    /// FK to `payout_cycle.id`.
    pub cycle_id: i64,
    /// FK to `wallet.id`.
    pub wallet_id: WalletId,
    /// Payout amount in sompi.
    pub amount_sompi: i64,
    /// Status state.
    pub status: PayoutStatus,
    /// KAS tx hash; populated for `Kas` cycle on submit.
    pub tx_hash: Option<Vec<u8>>,
    /// KRC-20 commit tx hash; populated on `commit_submitted`.
    pub krc20_commit_hash: Option<Vec<u8>>,
    /// KRC-20 reveal tx hash; populated on `reveal_submitted`.
    pub krc20_reveal_hash: Option<Vec<u8>>,
    /// When the payout row was created.
    pub planned_at: DateTime<Utc>,
    /// When the tx was submitted.
    pub submitted_at: Option<DateTime<Utc>>,
    /// When the tx was confirmed past maturity.
    pub confirmed_at: Option<DateTime<Utc>>,
    /// DAA score of the block that first carried this payout's treasury change
    /// coin (the accepting height). Recorded once the change coin is observed
    /// on chain, so confirmation can advance by depth even after that coin is
    /// later spent. `None` until first observed (and for KRC-20 rows).
    pub accepted_daa_score: Option<i64>,
    /// Why the payout failed (if `status = Failed`).
    pub failure_reason: Option<String>,
}

/// One row of `krc20_pending_transfer`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Krc20PendingTransfer {
    /// Synthetic primary key.
    pub id: i64,
    /// FK to `payout.id`.
    pub payout_id: i64,
    /// KAS sompi included in the commit tx (covers tx fees on reveal).
    pub sompi_to_miner: i64,
    /// NACHO integer-unit amount.
    pub nacho_amount: i64,
    /// P2SH address derived from the commit script.
    pub p2sh_address: String,
    /// State.
    pub status: Krc20TransferStatus,
    /// Frozen commit network fee (sompi). `None` until the transfer is first
    /// executed; persisted then so a crash-resume re-derives the same txid.
    pub commit_fee_sompi: Option<i64>,
    /// Frozen reveal network fee (sompi). `None` until the transfer is first
    /// executed; see [`Self::commit_fee_sompi`].
    pub reveal_fee_sompi: Option<i64>,
    /// Created-at timestamp.
    pub created_at: DateTime<Utc>,
    /// Updated-at timestamp.
    pub updated_at: DateTime<Utc>,
}

// ---- cycle ops ------------------------------------------------------

/// Compose the cycle's idempotency key from its (kind, daa range).
#[must_use]
pub fn idempotency_key(kind: PayoutKind, daa_start: DaaScore, daa_end: DaaScore) -> String {
    let prefix = match kind {
        PayoutKind::Kas => "kas",
        PayoutKind::Krc20Nacho => "krc20",
        PayoutKind::Zkas => "zkas",
    };
    format!("{prefix}-{}-{}", daa_start.value(), daa_end.value())
}

/// Create a cycle. Idempotent via the unique `idempotency_key`
/// column — calling twice with the same key returns the existing
/// row without conflict noise.
pub async fn create_cycle<'e, E>(
    executor: E,
    kind: PayoutKind,
    daa_start: DaaScore,
    daa_end: DaaScore,
) -> Result<PayoutCycle, DbError>
where
    E: PgExecutor<'e>,
{
    let key = idempotency_key(kind, daa_start, daa_end);
    sqlx::query_as::<_, PayoutCycle>(
        "INSERT INTO payout_cycle (kind, daa_start, daa_end, idempotency_key)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (idempotency_key) DO UPDATE
            SET idempotency_key = EXCLUDED.idempotency_key
         RETURNING id, kind, status, daa_start, daa_end, planned_at, broadcast_at,
                   settled_at, total_sompi, total_recipients, idempotency_key",
    )
    .bind(kind)
    .bind(daa_start.value() as i64)
    .bind(daa_end.value() as i64)
    .bind(key)
    .fetch_one(executor)
    .await
    .map_err(DbError::from)
}

/// Look up a cycle by its idempotency key.
pub async fn find_cycle_by_idempotency_key<'e, E: PgExecutor<'e>>(
    executor: E,
    key: &str,
) -> Result<Option<PayoutCycle>, DbError> {
    sqlx::query_as::<_, PayoutCycle>(
        "SELECT id, kind, status, daa_start, daa_end, planned_at, broadcast_at,
                settled_at, total_sompi, total_recipients, idempotency_key
           FROM payout_cycle
          WHERE idempotency_key = $1",
    )
    .bind(key)
    .fetch_optional(executor)
    .await
    .map_err(DbError::from)
}

/// Get a cycle by primary key.
pub async fn get_cycle<'e, E: PgExecutor<'e>>(
    executor: E,
    cycle_id: i64,
) -> Result<PayoutCycle, DbError> {
    sqlx::query_as::<_, PayoutCycle>(
        "SELECT id, kind, status, daa_start, daa_end, planned_at, broadcast_at,
                settled_at, total_sompi, total_recipients, idempotency_key
           FROM payout_cycle
          WHERE id = $1",
    )
    .bind(cycle_id)
    .fetch_one(executor)
    .await
    .map_err(DbError::from)
}

/// Advance a cycle to `broadcasting`. Idempotent.
pub async fn mark_cycle_broadcasting<'e, E: PgExecutor<'e>>(
    executor: E,
    cycle_id: i64,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE payout_cycle
            SET status = 'broadcasting',
                broadcast_at = COALESCE(broadcast_at, now())
          WHERE id = $1
            AND status IN ('planned', 'broadcasting')",
    )
    .bind(cycle_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Advance a cycle to `partially_settled`.
pub async fn mark_cycle_partially_settled<'e, E: PgExecutor<'e>>(
    executor: E,
    cycle_id: i64,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE payout_cycle
            SET status = 'partially_settled'
          WHERE id = $1
            AND status IN ('broadcasting', 'partially_settled')",
    )
    .bind(cycle_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Advance a cycle to `settled`.
pub async fn mark_cycle_settled<'e, E: PgExecutor<'e>>(
    executor: E,
    cycle_id: i64,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE payout_cycle
            SET status = 'settled',
                settled_at = COALESCE(settled_at, now())
          WHERE id = $1
            AND status IN ('broadcasting', 'partially_settled', 'settled')",
    )
    .bind(cycle_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Mark a cycle failed. Records a reason via the audit log; callers
/// should pair this with `repo::audit::append` for forensic detail.
pub async fn mark_cycle_failed<'e, E: PgExecutor<'e>>(
    executor: E,
    cycle_id: i64,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE payout_cycle
            SET status = 'failed'
          WHERE id = $1
            AND status <> 'settled'",
    )
    .bind(cycle_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Update the cycle totals after the planning step finalises
/// recipients.
pub async fn set_cycle_totals<'e, E: PgExecutor<'e>>(
    executor: E,
    cycle_id: i64,
    total_sompi: i64,
    total_recipients: i32,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE payout_cycle
            SET total_sompi = $2,
                total_recipients = $3
          WHERE id = $1",
    )
    .bind(cycle_id)
    .bind(total_sompi)
    .bind(total_recipients)
    .execute(executor)
    .await?;
    Ok(())
}

// ---- payout ops -----------------------------------------------------

/// Default minimum payable balance for a KAS payout (10 KAS).
pub const DEFAULT_KAS_PAYOUT_THRESHOLD_SOMPI: i64 = 1_000_000_000;

/// Wallet eligible for a KAS payout at the configured threshold.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct KasEligibleWallet {
    /// FK to `wallet.id`.
    pub wallet_id: WalletId,
    /// Bech32 payout address.
    pub address: String,
    /// Network slug (`mainnet`, `testnet-10`, …).
    pub network: String,
    /// Lifetime `sum(net_payout_sompi)` from `share_allocation`.
    pub allocated_sompi: i64,
    /// Confirmed KAS payouts, excluding legacy cutover imports.
    pub confirmed_paid_sompi: i64,
    /// `allocated_sompi - confirmed_paid_sompi`.
    pub payable_sompi: i64,
}

/// Wallets whose payable KAS balance meets `threshold_sompi`.
///
/// Payable balance is `sum(share_allocation.net_payout_sompi)` minus
/// `sum(payout.amount_sompi)` for confirmed rows in `kas` cycles only,
/// excluding legacy payouts imported at cutover (which settle pre-cutover
/// earnings not present in `share_allocation`).
/// In-flight (`planned` / `submitted`) payouts do not reduce payable
/// balance — the planner records idempotent rows before broadcast (M4.4).
pub async fn list_kas_eligible_wallets<'e, E: PgExecutor<'e>>(
    executor: E,
    threshold_sompi: i64,
) -> Result<Vec<KasEligibleWallet>, DbError> {
    sqlx::query_as::<_, KasEligibleWallet>(
        "SELECT w.id AS wallet_id,
                w.address,
                w.network,
                a.allocated_sompi,
                COALESCE(p.confirmed_paid_sompi, 0) AS confirmed_paid_sompi,
                a.allocated_sompi - COALESCE(p.confirmed_paid_sompi, 0) AS payable_sompi
           FROM wallet w
           INNER JOIN (
               SELECT wallet_id, sum(net_payout_sompi)::bigint AS allocated_sompi
                 FROM share_allocation
                GROUP BY wallet_id
           ) a ON a.wallet_id = w.id
           LEFT JOIN (
               SELECT po.wallet_id,
                      sum(po.amount_sompi)::bigint AS confirmed_paid_sompi
                 FROM payout po
                 INNER JOIN payout_cycle pc ON pc.id = po.cycle_id
                WHERE pc.kind = 'kas'
                  AND po.status = 'confirmed'
                  -- Exclude legacy payouts imported at cutover. They settle
                  -- pre-cutover earnings that were NOT imported into
                  -- share_allocation, so counting them here would subtract the
                  -- entire legacy payment history from a post-cutover-only
                  -- allocation total and drive payable deeply negative. The
                  -- importer tags every imported cycle `kind-legacy-<hash>`.
                  AND pc.idempotency_key NOT LIKE 'kas-legacy-%'
                GROUP BY po.wallet_id
           ) p ON p.wallet_id = w.id
          WHERE a.allocated_sompi - COALESCE(p.confirmed_paid_sompi, 0) >= $1
          ORDER BY payable_sompi DESC, w.id ASC",
    )
    .bind(threshold_sompi)
    .fetch_all(executor)
    .await
    .map_err(DbError::from)
}

/// Default minimum payable balance for a ZKas shielded payout (5 ZKAS).
///
/// Each recipient costs one full Orchard proof (seconds of CPU) plus the
/// shielded transaction fee, so dust-level balances are left to accrue.
pub const DEFAULT_ZKAS_PAYOUT_THRESHOLD_SOMPI: i64 = 500_000_000;

/// Wallets whose payable ZKas balance meets `threshold_sompi`.
///
/// Identical accounting shape to [`list_kas_eligible_wallets`] — payable is
/// `sum(share_allocation.net_payout_sompi)` minus confirmed payouts — but
/// counts `zkas` cycles (excluding any `zkas-legacy-%` cutover imports,
/// mirroring the KAS legacy rule). Reuses [`KasEligibleWallet`] as the row
/// shape; the fields are chain-agnostic. In-flight (`planned`/`submitted`)
/// payouts do not reduce payable balance — the planner's idempotent rows
/// prevent double-planning inside a cycle.
pub async fn list_zkas_eligible_wallets<'e, E: PgExecutor<'e>>(
    executor: E,
    threshold_sompi: i64,
) -> Result<Vec<KasEligibleWallet>, DbError> {
    sqlx::query_as::<_, KasEligibleWallet>(
        "SELECT w.id AS wallet_id,
                w.address,
                w.network,
                a.allocated_sompi,
                COALESCE(p.confirmed_paid_sompi, 0) AS confirmed_paid_sompi,
                a.allocated_sompi - COALESCE(p.confirmed_paid_sompi, 0) AS payable_sompi
           FROM wallet w
           INNER JOIN (
               SELECT wallet_id, sum(net_payout_sompi)::bigint AS allocated_sompi
                 FROM share_allocation
                GROUP BY wallet_id
           ) a ON a.wallet_id = w.id
           LEFT JOIN (
               SELECT po.wallet_id,
                      sum(po.amount_sompi)::bigint AS confirmed_paid_sompi
                 FROM payout po
                 INNER JOIN payout_cycle pc ON pc.id = po.cycle_id
                WHERE pc.kind = 'zkas'
                  AND po.status = 'confirmed'
                  AND pc.idempotency_key NOT LIKE 'zkas-legacy-%'
                GROUP BY po.wallet_id
           ) p ON p.wallet_id = w.id
          WHERE a.allocated_sompi - COALESCE(p.confirmed_paid_sompi, 0) >= $1
          ORDER BY payable_sompi DESC, w.id ASC",
    )
    .bind(threshold_sompi)
    .fetch_all(executor)
    .await
    .map_err(DbError::from)
}

/// A wallet eligible for a KRC-20 NACHO rebate payout, with the KAS-sompi
/// balance available to convert this cycle.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Krc20EligibleWallet {
    /// FK to `wallet.id`.
    pub wallet_id: WalletId,
    /// Bech32 payout address (also the inscription `to`).
    pub address: String,
    /// Network slug (`mainnet`, `testnet-10`, …).
    pub network: String,
    /// Pending rebate available this cycle, in KAS-sompi: accrued − paid −
    /// sompi already committed to non-terminal KRC-20 payouts.
    pub pending_sompi: i64,
}

/// List wallets eligible for a KRC-20 NACHO payout, descending by amount.
///
/// Pending balance is `nacho_rebate_accrual.accrued − paid` **minus** the
/// sompi already committed to KRC-20 payouts that are neither `confirmed`
/// (already reflected in `paid` after crediting) nor `failed` (refunded for
/// a future cycle). This netting makes a wallet with an in-flight transfer
/// un-selectable until it settles, so no cycle can double-select a balance.
pub async fn list_krc20_eligible_wallets<'e, E: PgExecutor<'e>>(
    executor: E,
    min_pending_sompi: i64,
    limit: i64,
) -> Result<Vec<Krc20EligibleWallet>, DbError> {
    sqlx::query_as::<_, Krc20EligibleWallet>(
        "SELECT r.wallet_id AS wallet_id,
                w.address AS address,
                w.network AS network,
                (r.accrued_sompi - r.paid_sompi - COALESCE(open.open_sompi, 0))::bigint
                    AS pending_sompi
           FROM nacho_rebate_accrual r
           INNER JOIN wallet w ON w.id = r.wallet_id
           LEFT JOIN (
               SELECT po.wallet_id,
                      sum(po.amount_sompi)::bigint AS open_sompi
                 FROM payout po
                 INNER JOIN payout_cycle pc ON pc.id = po.cycle_id
                WHERE pc.kind = 'krc20_nacho'
                  AND po.status NOT IN ('failed', 'confirmed')
                GROUP BY po.wallet_id
           ) open ON open.wallet_id = r.wallet_id
          WHERE (r.accrued_sompi - r.paid_sompi - COALESCE(open.open_sompi, 0)) >= $1
          ORDER BY pending_sompi DESC, r.wallet_id ASC
          LIMIT $2",
    )
    .bind(min_pending_sompi)
    .bind(limit)
    .fetch_all(executor)
    .await
    .map_err(DbError::from)
}

/// Insert a planned payout for a recipient under a cycle.
///
/// Prefer [`ensure_payout`] when the cycle may be retried — this
/// function surfaces `23505` on duplicate `(cycle_id, wallet_id)`.
pub async fn insert_payout<'e, E>(
    executor: E,
    cycle_id: i64,
    wallet_id: WalletId,
    amount_sompi: i64,
) -> Result<Payout, DbError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, Payout>(
        "INSERT INTO payout (cycle_id, wallet_id, amount_sompi)
         VALUES ($1, $2, $3)
         RETURNING id, cycle_id, wallet_id, amount_sompi, status, tx_hash,
                   krc20_commit_hash, krc20_reveal_hash, planned_at, submitted_at,
                   confirmed_at, accepted_daa_score, failure_reason",
    )
    .bind(cycle_id)
    .bind(wallet_id.0)
    .bind(amount_sompi)
    .fetch_one(executor)
    .await
    .map_err(DbError::from)
}

/// Insert or return the existing planned payout for `(cycle, wallet)`.
///
/// Idempotent across cycle replays: `ON CONFLICT` returns the row
/// without changing `amount_sompi` when it already exists.
pub async fn ensure_payout<'e, E>(
    executor: E,
    cycle_id: i64,
    wallet_id: WalletId,
    amount_sompi: i64,
) -> Result<Payout, DbError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, Payout>(
        "INSERT INTO payout (cycle_id, wallet_id, amount_sompi)
         VALUES ($1, $2, $3)
         ON CONFLICT (cycle_id, wallet_id) DO UPDATE
            SET amount_sompi = payout.amount_sompi
         RETURNING id, cycle_id, wallet_id, amount_sompi, status, tx_hash,
                   krc20_commit_hash, krc20_reveal_hash, planned_at, submitted_at,
                   confirmed_at, accepted_daa_score, failure_reason",
    )
    .bind(cycle_id)
    .bind(wallet_id.0)
    .bind(amount_sompi)
    .fetch_one(executor)
    .await
    .map_err(DbError::from)
}

/// Get a payout by primary key.
pub async fn get_payout<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
) -> Result<Payout, DbError> {
    sqlx::query_as::<_, Payout>(
        "SELECT id, cycle_id, wallet_id, amount_sompi, status, tx_hash,
                krc20_commit_hash, krc20_reveal_hash, planned_at, submitted_at,
                confirmed_at, accepted_daa_score, failure_reason
           FROM payout
          WHERE id = $1",
    )
    .bind(payout_id)
    .fetch_one(executor)
    .await
    .map_err(DbError::from)
}

/// A wallet's KAS payable position — the single-wallet analogue of one
/// [`list_kas_eligible_wallets`] row. Drives the Phase 6
/// `/api/v1/balance/:address` endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalletKasBalance {
    /// Lifetime `sum(share_allocation.net_payout_sompi)`.
    pub allocated_sompi: i64,
    /// Confirmed KAS payouts (`payout.amount_sompi`, `kas` cycles),
    /// excluding legacy cutover imports.
    pub confirmed_paid_sompi: i64,
    /// `allocated_sompi - confirmed_paid_sompi` — the unpaid KAS balance.
    pub payable_sompi: i64,
}

/// Compute a single wallet's KAS payable balance.
///
/// Identical accounting to [`list_kas_eligible_wallets`] (allocated minus
/// confirmed `kas`-cycle payouts; in-flight payouts do **not** reduce it),
/// scoped to one wallet and returning a row even when the wallet has no
/// allocations (all-zero), so the API can distinguish "known wallet, zero
/// balance" from "unknown wallet" (the latter is caught earlier by
/// `wallet::find_by_address`).
pub async fn kas_payable_for_wallet<'e, E: PgExecutor<'e>>(
    executor: E,
    wallet_id: WalletId,
) -> Result<WalletKasBalance, DbError> {
    let row: (i64, i64) = sqlx::query_as(
        "SELECT
           COALESCE((SELECT sum(net_payout_sompi)::bigint
                       FROM share_allocation
                      WHERE wallet_id = $1), 0),
           COALESCE((SELECT sum(po.amount_sompi)::bigint
                       FROM payout po
                       INNER JOIN payout_cycle pc ON pc.id = po.cycle_id
                      WHERE pc.kind = 'kas'
                        AND po.status = 'confirmed'
                        -- Exclude legacy payouts imported at cutover (they
                        -- settle pre-cutover earnings absent from
                        -- share_allocation); see list_kas_eligible_wallets.
                        AND pc.idempotency_key NOT LIKE 'kas-legacy-%'
                        AND po.wallet_id = $1), 0)",
    )
    .bind(wallet_id.0)
    .fetch_one(executor)
    .await?;
    Ok(WalletKasBalance {
        allocated_sompi: row.0,
        confirmed_paid_sompi: row.1,
        payable_sompi: row.0 - row.1,
    })
}

/// Pool-wide confirmed-payout totals, split by kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolPayoutTotals {
    /// Sum of confirmed KAS payout amounts (sompi).
    pub kas_confirmed_sompi: i64,
    /// Sum of confirmed KRC-20/NACHO payout amounts, denominated in the
    /// KAS-sompi value that was converted (not NACHO base units).
    pub nacho_confirmed_sompi: i64,
    /// Count of confirmed payout rows across both kinds.
    pub confirmed_payouts: i64,
}

/// Aggregate confirmed-payout totals across the whole pool. Drives the
/// pool-stats "total paid" figures.
pub async fn pool_payout_totals<'e, E: PgExecutor<'e>>(
    executor: E,
) -> Result<PoolPayoutTotals, DbError> {
    let row: (Option<i64>, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT
           COALESCE(sum(CASE WHEN pc.kind = 'kas' AND po.status = 'confirmed'
                             THEN po.amount_sompi ELSE 0 END), 0)::bigint,
           COALESCE(sum(CASE WHEN pc.kind = 'krc20_nacho' AND po.status = 'confirmed'
                             THEN po.amount_sompi ELSE 0 END), 0)::bigint,
           COALESCE(sum(CASE WHEN po.status = 'confirmed' THEN 1 ELSE 0 END), 0)::bigint
           FROM payout po
           INNER JOIN payout_cycle pc ON pc.id = po.cycle_id",
    )
    .fetch_one(executor)
    .await?;
    Ok(PoolPayoutTotals {
        kas_confirmed_sompi: row.0.unwrap_or(0),
        nacho_confirmed_sompi: row.1.unwrap_or(0),
        confirmed_payouts: row.2.unwrap_or(0),
    })
}

/// Recent payout cycles across both kinds, newest-first, keyset-paginated
/// (`before_id = None` for the first page; the previous page's smallest
/// `id` for the next).
pub async fn list_recent_cycles<'e, E: PgExecutor<'e>>(
    executor: E,
    limit: i64,
    before_id: Option<i64>,
) -> Result<Vec<PayoutCycle>, DbError> {
    sqlx::query_as::<_, PayoutCycle>(
        "SELECT id, kind, status, daa_start, daa_end, planned_at, broadcast_at,
                settled_at, total_sompi, total_recipients, idempotency_key
           FROM payout_cycle
          WHERE ($2::bigint IS NULL OR id < $2)
          ORDER BY id DESC
          LIMIT $1",
    )
    .bind(limit)
    .bind(before_id)
    .fetch_all(executor)
    .await
    .map_err(DbError::from)
}

/// One row of a wallet's payout history, joined with its cycle's `kind`
/// so the API can label KAS vs NACHO without a second query.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WalletPayout {
    /// `payout.id`.
    pub id: i64,
    /// FK to `payout_cycle.id`.
    pub cycle_id: i64,
    /// The owning cycle's kind.
    pub kind: PayoutKind,
    /// Payout amount in sompi.
    pub amount_sompi: i64,
    /// Per-recipient status.
    pub status: PayoutStatus,
    /// KAS tx hash (KAS cycles).
    pub tx_hash: Option<Vec<u8>>,
    /// KRC-20 commit tx hash (NACHO cycles).
    pub krc20_commit_hash: Option<Vec<u8>>,
    /// KRC-20 reveal tx hash (NACHO cycles).
    pub krc20_reveal_hash: Option<Vec<u8>>,
    /// When the payout row was created.
    pub planned_at: DateTime<Utc>,
    /// When the tx was submitted.
    pub submitted_at: Option<DateTime<Utc>>,
    /// When the tx confirmed past maturity.
    pub confirmed_at: Option<DateTime<Utc>>,
    /// Failure reason if `status = failed`.
    pub failure_reason: Option<String>,
    /// NACHO base units for KRC-20 rebate payouts (`None` for KAS cycles).
    pub nacho_amount: Option<i64>,
}

/// A wallet's payout history (both kinds), newest-first, keyset-paginated.
pub async fn list_for_wallet_detailed<'e, E: PgExecutor<'e>>(
    executor: E,
    wallet_id: WalletId,
    limit: i64,
    before_id: Option<i64>,
) -> Result<Vec<WalletPayout>, DbError> {
    sqlx::query_as::<_, WalletPayout>(
        "SELECT po.id, po.cycle_id, pc.kind, po.amount_sompi, po.status,
                po.tx_hash, po.krc20_commit_hash, po.krc20_reveal_hash,
                po.planned_at, po.submitted_at, po.confirmed_at, po.failure_reason,
                k.nacho_amount
           FROM payout po
           INNER JOIN payout_cycle pc ON pc.id = po.cycle_id
           LEFT JOIN krc20_pending_transfer k ON k.payout_id = po.id
          WHERE po.wallet_id = $1
            AND ($3::bigint IS NULL OR po.id < $3)
          ORDER BY po.id DESC
          LIMIT $2",
    )
    .bind(wallet_id.0)
    .bind(limit)
    .bind(before_id)
    .fetch_all(executor)
    .await
    .map_err(DbError::from)
}

/// List every payout under a cycle.
pub async fn list_for_cycle<'e, E: PgExecutor<'e>>(
    executor: E,
    cycle_id: i64,
) -> Result<Vec<Payout>, DbError> {
    sqlx::query_as::<_, Payout>(
        "SELECT id, cycle_id, wallet_id, amount_sompi, status, tx_hash,
                krc20_commit_hash, krc20_reveal_hash, planned_at, submitted_at,
                confirmed_at, accepted_daa_score, failure_reason
           FROM payout
          WHERE cycle_id = $1
          ORDER BY amount_sompi DESC, id ASC",
    )
    .bind(cycle_id)
    .fetch_all(executor)
    .await
    .map_err(DbError::from)
}

/// One in-flight (submitted/accepted) ZKas payout awaiting confirmation.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ZkasInFlightPayout {
    /// `payout.id`.
    pub payout_id: i64,
    /// Owning cycle.
    pub cycle_id: i64,
    /// Shielded tx hash recorded at submit (32 bytes).
    pub tx_hash: Vec<u8>,
    /// Accepting DAA score, if a prior pass observed acceptance.
    pub accepted_daa_score: Option<i64>,
}

/// Every submitted-but-unconfirmed ZKas payout, across **all** cycles, so
/// a straggler from a previous window (engine downtime over a rollover)
/// still gets confirmed instead of stranding forever.
pub async fn list_zkas_in_flight_payouts<'e, E: PgExecutor<'e>>(
    executor: E,
) -> Result<Vec<ZkasInFlightPayout>, DbError> {
    sqlx::query_as::<_, ZkasInFlightPayout>(
        "SELECT po.id AS payout_id, po.cycle_id, po.tx_hash, po.accepted_daa_score
           FROM payout po
           INNER JOIN payout_cycle pc ON pc.id = po.cycle_id
          WHERE pc.kind = 'zkas'
            AND po.status IN ('submitted', 'accepted')
            AND po.tx_hash IS NOT NULL
          ORDER BY po.id ASC",
    )
    .fetch_all(executor)
    .await
    .map_err(DbError::from)
}

/// One recipient row for a payout cycle detail view.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CycleRecipient {
    /// `payout.id`.
    pub payout_id: i64,
    /// Recipient wallet address.
    pub address: String,
    /// Rebate amount in KAS-sompi (the accrued balance paid out).
    pub amount_sompi: i64,
    /// Per-recipient payout status.
    pub status: PayoutStatus,
    /// KAS tx hash (KAS cycles).
    pub tx_hash: Option<Vec<u8>>,
    /// KRC-20 commit tx hash (NACHO cycles).
    pub krc20_commit_hash: Option<Vec<u8>>,
    /// KRC-20 reveal tx hash (NACHO cycles).
    pub krc20_reveal_hash: Option<Vec<u8>>,
    /// NACHO base units transferred (NACHO cycles only).
    pub nacho_amount: Option<i64>,
}

/// Every recipient under a cycle, with wallet address and optional NACHO
/// token amount, ordered largest rebate first.
pub async fn list_cycle_recipients<'e, E: PgExecutor<'e>>(
    executor: E,
    cycle_id: i64,
) -> Result<Vec<CycleRecipient>, DbError> {
    sqlx::query_as::<_, CycleRecipient>(
        "SELECT po.id AS payout_id, w.address, po.amount_sompi, po.status,
                po.tx_hash, po.krc20_commit_hash, po.krc20_reveal_hash,
                k.nacho_amount
           FROM payout po
           INNER JOIN wallet w ON w.id = po.wallet_id
           LEFT JOIN krc20_pending_transfer k ON k.payout_id = po.id
          WHERE po.cycle_id = $1
          ORDER BY po.amount_sompi DESC, po.id ASC",
    )
    .bind(cycle_id)
    .fetch_all(executor)
    .await
    .map_err(DbError::from)
}

/// Recent payouts for one wallet.
pub async fn list_for_wallet<'e, E: PgExecutor<'e>>(
    executor: E,
    wallet_id: WalletId,
    limit: i64,
) -> Result<Vec<Payout>, DbError> {
    sqlx::query_as::<_, Payout>(
        "SELECT id, cycle_id, wallet_id, amount_sompi, status, tx_hash,
                krc20_commit_hash, krc20_reveal_hash, planned_at, submitted_at,
                confirmed_at, accepted_daa_score, failure_reason
           FROM payout
          WHERE wallet_id = $1
          ORDER BY planned_at DESC
          LIMIT $2",
    )
    .bind(wallet_id.0)
    .bind(limit)
    .fetch_all(executor)
    .await
    .map_err(DbError::from)
}

/// On-chain tx hashes of every payout not yet in a terminal state
/// (`confirmed`/`failed`) — KAS `tx_hash` plus KRC-20 commit/reveal hashes.
///
/// The consolidation engine must not spend a treasury coin produced by one of
/// these transactions (the payout's change output): confirmation detects
/// acceptance from that change coin, so sweeping it before the payout settles
/// would strand the payout. Returns raw 32-byte hashes; callers match them
/// against each spendable UTXO's `transaction_id`.
pub async fn in_flight_spend_tx_hashes<'e, E: PgExecutor<'e>>(
    executor: E,
) -> Result<Vec<Vec<u8>>, DbError> {
    sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT h
           FROM payout p
           CROSS JOIN LATERAL (
               VALUES (p.tx_hash), (p.krc20_commit_hash), (p.krc20_reveal_hash)
           ) AS v(h)
          WHERE p.status NOT IN ('confirmed', 'failed')
            AND h IS NOT NULL",
    )
    .fetch_all(executor)
    .await
    .map_err(DbError::from)
}

/// Mark a KAS payout submitted, recording the on-chain tx hash.
pub async fn mark_payout_submitted<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
    tx_hash: BlockHash,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE payout
            SET status = 'submitted',
                tx_hash = $2,
                submitted_at = COALESCE(submitted_at, now())
          WHERE id = $1
            AND status IN ('planned', 'submitted')",
    )
    .bind(payout_id)
    .bind(tx_hash.as_bytes().to_vec())
    .execute(executor)
    .await?;
    Ok(())
}

/// Mark a payout accepted, durably recording its accepting DAA score.
///
/// The score (the change coin's `block_daa_score`) is written first-write-wins
/// so later passes can confirm by depth even after that coin is spent. Idempotent
/// and monotonic: only advances from `submitted`/`accepted`, never regresses a
/// `confirmed` row, and never moves an already-recorded accepting height.
pub async fn mark_payout_accepted<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
    accepted_daa_score: i64,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE payout
            SET status = 'accepted',
                accepted_daa_score = COALESCE(accepted_daa_score, $2)
          WHERE id = $1
            AND status IN ('submitted', 'accepted')",
    )
    .bind(payout_id)
    .bind(accepted_daa_score)
    .execute(executor)
    .await?;
    Ok(())
}

/// Mark a payout confirmed past maturity window.
pub async fn mark_payout_confirmed<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE payout
            SET status = 'confirmed',
                confirmed_at = COALESCE(confirmed_at, now())
          WHERE id = $1
            AND status IN ('submitted', 'accepted', 'confirmed')",
    )
    .bind(payout_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Confirm a KRC-20 payout exactly once, returning whether *this* call
/// performed the transition.
///
/// KRC-20 payout rows stay `planned` while their commit/reveal progresses in
/// `krc20_pending_transfer` (the executor never touches `payout.status`), so
/// crediting transitions `planned → confirmed` directly. The boolean return
/// (`true` only when a row actually changed) is the exactly-once guard the
/// caller uses to credit `nacho_rebate.paid_sompi` in the same transaction —
/// a re-run finds the row already `confirmed`, returns `false`, and skips the
/// (additive) credit.
pub async fn confirm_krc20_payout_once<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
) -> Result<bool, DbError> {
    // KRC-20 payouts go planned → confirmed (the commit/reveal lifecycle lives
    // in krc20_pending_transfer), so stamp submitted_at alongside confirmed_at
    // to satisfy the payout_lifecycle_order CHECK.
    let result = sqlx::query(
        "UPDATE payout
            SET status = 'confirmed',
                submitted_at = COALESCE(submitted_at, now()),
                confirmed_at = COALESCE(confirmed_at, now())
          WHERE id = $1
            AND status <> 'confirmed'",
    )
    .bind(payout_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Mark a payout failed with a reason. Failed payouts can be retried
/// by re-planning the recipient in a fresh cycle.
pub async fn mark_payout_failed<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
    reason: &str,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE payout
            SET status = 'failed',
                failure_reason = $2
          WHERE id = $1
            AND status <> 'confirmed'",
    )
    .bind(payout_id)
    .bind(reason)
    .execute(executor)
    .await?;
    Ok(())
}

// ---- KRC-20 ops -----------------------------------------------------

/// Open a new pending KRC-20 transfer associated with a payout row.
/// One-to-one with the parent payout.
pub async fn insert_krc20_pending<'e, E>(
    executor: E,
    payout_id: i64,
    sompi_to_miner: i64,
    nacho_amount: i64,
    p2sh_address: &str,
) -> Result<Krc20PendingTransfer, DbError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, Krc20PendingTransfer>(
        "INSERT INTO krc20_pending_transfer
            (payout_id, sompi_to_miner, nacho_amount, p2sh_address)
         VALUES ($1, $2, $3, $4)
         RETURNING id, payout_id, sompi_to_miner, nacho_amount, p2sh_address,
                   status, commit_fee_sompi, reveal_fee_sompi, created_at, updated_at",
    )
    .bind(payout_id)
    .bind(sompi_to_miner)
    .bind(nacho_amount)
    .bind(p2sh_address)
    .fetch_one(executor)
    .await
    .map_err(DbError::from)
}

/// Idempotently open a pending KRC-20 transfer for a payout row.
///
/// Unlike [`insert_krc20_pending`], a re-run for the same `payout_id`
/// refreshes the planned amounts/address (deterministic for identical
/// inputs) instead of violating the one-to-one UNIQUE constraint — so
/// re-planning a cycle is safe.
pub async fn ensure_krc20_pending<'e, E>(
    executor: E,
    payout_id: i64,
    sompi_to_miner: i64,
    nacho_amount: i64,
    p2sh_address: &str,
) -> Result<Krc20PendingTransfer, DbError>
where
    E: PgExecutor<'e>,
{
    sqlx::query_as::<_, Krc20PendingTransfer>(
        "INSERT INTO krc20_pending_transfer
            (payout_id, sompi_to_miner, nacho_amount, p2sh_address)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (payout_id) DO UPDATE
            SET sompi_to_miner = EXCLUDED.sompi_to_miner,
                nacho_amount = EXCLUDED.nacho_amount,
                p2sh_address = EXCLUDED.p2sh_address,
                updated_at = now()
         RETURNING id, payout_id, sompi_to_miner, nacho_amount, p2sh_address,
                   status, commit_fee_sompi, reveal_fee_sompi, created_at, updated_at",
    )
    .bind(payout_id)
    .bind(sompi_to_miner)
    .bind(nacho_amount)
    .bind(p2sh_address)
    .fetch_one(executor)
    .await
    .map_err(DbError::from)
}

/// Record the KRC-20 **commit** tx hash on the parent payout row.
///
/// Written *before* broadcast (paired with [`mark_krc20_commit_submitted`]
/// in one transaction) so a crash mid-broadcast is recoverable: the
/// deterministic txid (sig scripts excluded) re-derives identically from the
/// same inputs on resume.
pub async fn record_krc20_commit_hash<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
    commit_hash: BlockHash,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE payout
            SET krc20_commit_hash = $2
          WHERE id = $1",
    )
    .bind(payout_id)
    .bind(commit_hash.as_bytes().to_vec())
    .execute(executor)
    .await?;
    Ok(())
}

/// Record the KRC-20 **reveal** tx hash on the parent payout row.
///
/// Written *before* broadcast (paired with [`mark_krc20_reveal_submitted`]
/// in one transaction), same crash-safe rationale as
/// [`record_krc20_commit_hash`].
pub async fn record_krc20_reveal_hash<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
    reveal_hash: BlockHash,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE payout
            SET krc20_reveal_hash = $2
          WHERE id = $1",
    )
    .bind(payout_id)
    .bind(reveal_hash.as_bytes().to_vec())
    .execute(executor)
    .await?;
    Ok(())
}

/// Freeze the resolved commit/reveal network fees on a transfer row.
///
/// Written once, in the same transaction as [`record_krc20_commit_hash`] /
/// [`mark_krc20_commit_submitted`], *before* the commit is broadcast. The
/// commit `change` and reveal `return` values (hence both txids) derive from
/// these fees, so persisting them lets a crash-resume reconstruct the exact
/// same transactions instead of re-quoting a drifted node fee-rate. The guard
/// only writes when both columns are still NULL, so the first executor to
/// record wins and later reconstructions never overwrite the frozen value.
pub async fn record_krc20_fees<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
    commit_fee_sompi: i64,
    reveal_fee_sompi: i64,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE krc20_pending_transfer
            SET commit_fee_sompi = $2,
                reveal_fee_sompi = $3,
                updated_at = now()
          WHERE payout_id = $1
            AND commit_fee_sompi IS NULL
            AND reveal_fee_sompi IS NULL",
    )
    .bind(payout_id)
    .bind(commit_fee_sompi)
    .bind(reveal_fee_sompi)
    .execute(executor)
    .await?;
    Ok(())
}

/// Advance a transfer to `commit_submitted`.
pub async fn mark_krc20_commit_submitted<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE krc20_pending_transfer
            SET status = 'commit_submitted',
                updated_at = now()
          WHERE payout_id = $1
            AND status IN ('pending', 'commit_submitted')",
    )
    .bind(payout_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Advance a transfer to `reveal_submitted`.
pub async fn mark_krc20_reveal_submitted<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE krc20_pending_transfer
            SET status = 'reveal_submitted',
                updated_at = now()
          WHERE payout_id = $1
            AND status IN ('commit_submitted', 'reveal_submitted')",
    )
    .bind(payout_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Advance a transfer to `completed`.
pub async fn mark_krc20_completed<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE krc20_pending_transfer
            SET status = 'completed',
                updated_at = now()
          WHERE payout_id = $1
            AND status IN ('reveal_submitted', 'completed')",
    )
    .bind(payout_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Mark a transfer failed.
pub async fn mark_krc20_failed<'e, E: PgExecutor<'e>>(
    executor: E,
    payout_id: i64,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE krc20_pending_transfer
            SET status = 'failed',
                updated_at = now()
          WHERE payout_id = $1
            AND status <> 'completed'",
    )
    .bind(payout_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// List KRC-20 transfers in any of the given statuses.
pub async fn list_krc20_by_status<'e, E: PgExecutor<'e>>(
    executor: E,
    statuses: &[Krc20TransferStatus],
    limit: i64,
) -> Result<Vec<Krc20PendingTransfer>, DbError> {
    sqlx::query_as::<_, Krc20PendingTransfer>(
        "SELECT id, payout_id, sompi_to_miner, nacho_amount, p2sh_address,
                status, commit_fee_sompi, reveal_fee_sompi, created_at, updated_at
           FROM krc20_pending_transfer
          WHERE status = ANY($1)
          ORDER BY created_at ASC
          LIMIT $2",
    )
    .bind(statuses)
    .bind(limit)
    .fetch_all(executor)
    .await
    .map_err(DbError::from)
}

/// All KRC-20 transfers belonging to a payout cycle, oldest first.
pub async fn list_krc20_for_cycle<'e, E: PgExecutor<'e>>(
    executor: E,
    cycle_id: i64,
) -> Result<Vec<Krc20PendingTransfer>, DbError> {
    sqlx::query_as::<_, Krc20PendingTransfer>(
        "SELECT k.id, k.payout_id, k.sompi_to_miner, k.nacho_amount, k.p2sh_address,
                k.status, k.commit_fee_sompi, k.reveal_fee_sompi, k.created_at, k.updated_at
           FROM krc20_pending_transfer k
           INNER JOIN payout p ON p.id = k.payout_id
          WHERE p.cycle_id = $1
          ORDER BY k.created_at ASC, k.id ASC",
    )
    .bind(cycle_id)
    .fetch_all(executor)
    .await
    .map_err(DbError::from)
}
