//! ZKas payout cycle planning (database only; no chain, no prover).

use katpool_db::DbError;
use katpool_db::repo::payout::{self, PayoutCycle, PayoutKind};
use katpool_domain::DaaScore;
use sqlx::PgPool;

use crate::engine::EngineError;

/// Ceiling on one wallet's payout per cycle (300 ZKAS).
///
/// A single shielded transaction can spend at most six notes (standard-mass
/// cap), and the treasury's notes are 60-ZKAS coinbases, so one send tops
/// out just under 6 × 60 ZKAS. Amounts above the cap are simply deferred:
/// eligibility is `allocated − confirmed`, so the remainder is planned by
/// the next cycle automatically and a large balance drains cap-per-cycle.
pub const DEFAULT_PER_WALLET_CAP_SOMPI: i64 = 30_000_000_000;

/// Parameters for [`plan_zkas_cycle`].
#[derive(Debug, Clone, Copy)]
pub struct PlanZkasCycleParams {
    /// Half-open DAA range start (inclusive).
    pub daa_start: DaaScore,
    /// Half-open DAA range end (exclusive).
    pub daa_end: DaaScore,
    /// Minimum payable sompi per wallet.
    pub threshold_sompi: i64,
    /// Per-wallet per-cycle ceiling (see [`DEFAULT_PER_WALLET_CAP_SOMPI`]).
    pub per_wallet_cap_sompi: i64,
}

/// Outcome of a successful planning pass.
#[derive(Debug, Clone)]
pub struct PlanZkasCycleResult {
    /// The cycle row (`planned` status, or resumed).
    pub cycle: PayoutCycle,
    /// Recipients planned (rows ensured this pass).
    pub payouts_planned: u64,
    /// Total sompi across planned recipients.
    pub total_sompi: i64,
}

/// Create (or resume) a ZKas payout cycle and insert planned payout rows —
/// each eligible wallet's **full payable balance** (no vesting split).
///
/// Idempotent on retry: `create_cycle` is keyed by the `zkas-{start}-{end}`
/// idempotency key and `ensure_payout` by `(cycle_id, wallet_id)`; totals
/// are recomputed from the final recipient set.
pub async fn plan_zkas_cycle(
    pool: &PgPool,
    params: PlanZkasCycleParams,
) -> Result<PlanZkasCycleResult, EngineError> {
    let mut tx = pool.begin().await.map_err(DbError::from)?;

    let cycle =
        payout::create_cycle(&mut *tx, PayoutKind::Zkas, params.daa_start, params.daa_end).await?;

    let eligible = payout::list_zkas_eligible_wallets(&mut *tx, params.threshold_sompi).await?;

    let cap = params.per_wallet_cap_sompi.max(1);
    let mut total_sompi = 0_i64;
    let mut total_recipients = 0_i32;
    for wallet in &eligible {
        // Clamp to the single-transaction spend capacity; the remainder
        // stays payable and rolls into the next cycle.
        let amount = wallet.payable_sompi.min(cap);
        payout::ensure_payout(&mut *tx, cycle.id, wallet.wallet_id, amount).await?;
        total_sompi = total_sompi.saturating_add(amount);
        total_recipients = total_recipients.saturating_add(1);
    }

    payout::set_cycle_totals(&mut *tx, cycle.id, total_sompi, total_recipients).await?;

    tx.commit().await.map_err(DbError::from)?;

    Ok(PlanZkasCycleResult {
        cycle,
        payouts_planned: eligible.len() as u64,
        total_sompi,
    })
}
