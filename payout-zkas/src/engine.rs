//! The ZKas shielded payout engine: a single-leader periodic loop that pays
//! each eligible miner's **full accrued balance** (no vesting, no claim, no
//! signature) with one shielded transaction per recipient.
//!
//! ## Cycle model
//!
//! Identical to `payout-kas`: the cycle identity is the DAA bucket
//! (`cycle_window`), so every tick inside one bucket resumes the same cycle
//! and its idempotency key `zkas-{start}-{end}`; `ensure_payout` is keyed by
//! `(cycle_id, wallet_id)` so re-planning never duplicates a recipient.
//!
//! ## Money-safety properties
//!
//! - **Single leader** via the shared `treasury:spend-leader` advisory lock.
//! - **Spend cap**: a cycle whose planned total exceeds the operator cap is
//!   never broadcast.
//! - **In-flight latch**: before each subprocess spend, the recipient's
//!   `payout.id` is written to `pool_meta` (key [`INFLIGHT_KEY`]) and cleared
//!   only after the txid is durably recorded (or the failure is provably
//!   pre-submission). If the engine crashes mid-send, or the sender reports
//!   an *ambiguous* outcome (timeout / txid unparsable), the latch stays set
//!   and **all broadcasting halts** until an operator reconciles — because a
//!   blind retry after a possibly-accepted spend is the double-pay path.
//!   Reconcile runbook: check whether the recipient's balance/explorer shows
//!   the payment; if NOT paid, `DELETE FROM pool_meta WHERE key =
//!   'zkas_payout_inflight'`; if paid, also mark the payout row submitted +
//!   confirmed with the observed txid before deleting the latch.
//! - **Confirmation from consensus, not absence**: acceptance is read from
//!   the virtual chain's accepted-transaction ids (cursor persisted in
//!   `pool_meta`); a transaction that is neither in the mempool nor observed
//!   accepted is `Unknown` and left untouched — never auto-failed, never
//!   re-sent.
//! - **Serial sends, bounded per tick**: each send is a full Orchard proof;
//!   `max_sends_per_tick` bounds a tick's wall time, the remainder resumes
//!   next tick.

use std::collections::HashSet;
use std::time::Duration;

use kaspa_hashes::Hash as KaspaHash;
use katpool_db::repo::payout::{
    self, CycleRecipient, PayoutCycle, PayoutCycleStatus, PayoutKind, PayoutStatus,
};
use katpool_db::repo::pool_meta;
use katpool_idempotency::{AdvisoryLock, advisory_key};
use sqlx::PgPool;
use tokio::sync::watch;
use tokio::time;
use tracing::{error, info, warn};

use crate::chain::{ChainError, ChainReader};
use crate::confirm::{ConfirmationInputs, ConfirmationState, ZKAS_PAYOUT_CONFIRMATION_DAA, classify_confirmation};
use crate::plan::{PlanZkasCycleParams, plan_zkas_cycle};
use crate::sender::ShieldedSender;
use crate::window::cycle_window;

/// `pool_meta` key latched to the payout id currently being sent.
pub const INFLIGHT_KEY: &str = "zkas_payout_inflight";

/// `pool_meta` key holding the confirmation cursor (chain-block hash, hex).
pub const CONFIRM_CURSOR_KEY: &str = "zkas_payout_confirm_cursor";

/// Whether `total` exceeds the optional per-cycle treasury spend cap.
#[must_use]
pub const fn over_spend_cap(total: i64, cap: Option<i64>) -> bool {
    matches!(cap, Some(c) if total > c)
}

/// Broadcast behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Plan + confirm, log every would-send, never spawn the sender.
    DryRun,
    /// Really send.
    Live,
}

/// Engine configuration.
#[derive(Debug, Clone)]
pub struct ZkasPayoutEngineConfig {
    /// Tick cadence.
    pub tick_interval: Duration,
    /// DAA width of one payout cycle (must exceed
    /// [`ZKAS_PAYOUT_CONFIRMATION_DAA`]). `3_600` ≈ hourly at 1 BPS.
    pub cycle_span_daa: u64,
    /// Minimum payable balance per recipient.
    pub threshold_sompi: i64,
    /// Per-wallet per-cycle payout ceiling (single-tx spend capacity; see
    /// [`crate::plan::DEFAULT_PER_WALLET_CAP_SOMPI`]).
    pub per_wallet_cap_sompi: i64,
    /// Optional per-cycle total spend ceiling.
    pub spend_cap_sompi: Option<i64>,
    /// Dry-run vs live.
    pub mode: ExecutionMode,
    /// Max shielded sends (proofs) per tick.
    pub max_sends_per_tick: usize,
    /// Advisory-lock namespace (shared with the other treasury spenders).
    pub lock_namespace: String,
    /// Metric/audit instance label.
    pub instance_id: String,
}

/// Errors from the engine.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Database / advisory-lock failure.
    #[error(transparent)]
    Db(#[from] katpool_db::DbError),
    /// Chain read failure.
    #[error(transparent)]
    Chain(#[from] ChainError),
    /// `cycle_span_daa` too small to confirm in-window.
    #[error("cycle_span_daa ({span}) must exceed confirmation depth ({depth})")]
    SpanTooSmall {
        /// Configured span.
        span: u64,
        /// Required depth.
        depth: u64,
    },
}

/// The engine. Construct with [`Self::new`], drive with [`Self::run_loop`]
/// (or [`Self::tick_once`] for tests / the operator one-shot command).
pub struct ZkasPayoutEngine {
    db: PgPool,
    sender: Box<dyn ShieldedSender>,
    chain: Box<dyn ChainReader>,
    cfg: ZkasPayoutEngineConfig,
    lock_key: i64,
}

/// Outcome counters for one tick (operator-facing).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TickStats {
    /// Recipients newly planned this tick.
    pub planned: u64,
    /// Sends attempted this tick.
    pub sends_attempted: u64,
    /// Sends accepted by the node (txid recorded).
    pub sends_accepted: u64,
    /// Payouts newly observed accepted on chain.
    pub accepted: u64,
    /// Payouts newly confirmed past depth.
    pub confirmed: u64,
    /// True when broadcasting was skipped because the in-flight latch is set.
    pub inflight_latched: bool,
    /// True when broadcasting was skipped by the spend cap.
    pub spend_capped: bool,
}

impl ZkasPayoutEngine {
    /// Construct, validating the span/confirmation relationship.
    pub fn new(
        db: PgPool,
        sender: Box<dyn ShieldedSender>,
        chain: Box<dyn ChainReader>,
        cfg: ZkasPayoutEngineConfig,
    ) -> Result<Self, EngineError> {
        if cfg.cycle_span_daa <= ZKAS_PAYOUT_CONFIRMATION_DAA {
            return Err(EngineError::SpanTooSmall {
                span: cfg.cycle_span_daa,
                depth: ZKAS_PAYOUT_CONFIRMATION_DAA,
            });
        }
        let lock_key = advisory_key(&cfg.lock_namespace);
        Ok(Self { db, sender, chain, cfg, lock_key })
    }

    /// Run ticks until `shutdown` flips.
    pub async fn run_loop(self, mut shutdown: watch::Receiver<bool>) -> Result<(), EngineError> {
        // Anchor the confirmation cursor before the first possible send so
        // no accepted payout can ever predate the cursor.
        self.ensure_confirm_cursor().await?;
        let mut interval = time::interval(self.cfg.tick_interval);
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(instance = %self.cfg.instance_id, "zkas payout engine: shutdown");
                        return Ok(());
                    }
                }
                _ = interval.tick() => {
                    match self.tick_once().await {
                        Ok(stats) => info!(instance = %self.cfg.instance_id, ?stats, "zkas payout tick done"),
                        Err(e) => warn!(instance = %self.cfg.instance_id, error = %e, "zkas payout tick failed; will retry"),
                    }
                }
            }
        }
    }

    /// One leader-guarded tick: resume-or-plan the bucket's cycle, broadcast
    /// planned payouts (unless capped/latched/dry-run), then run the
    /// confirmation pass and reconcile cycle status.
    pub async fn tick_once(&self) -> Result<TickStats, EngineError> {
        let Some(_lock) = AdvisoryLock::try_acquire(&self.db, self.lock_key).await? else {
            info!(instance = %self.cfg.instance_id, "zkas payout: not leader this tick");
            return Ok(TickStats::default());
        };

        let mut stats = TickStats::default();
        let virtual_daa = self.chain.virtual_daa_score().await?;
        let (start, end) = cycle_window(virtual_daa, self.cfg.cycle_span_daa);

        // ---- resume or plan the bucket's cycle -----------------------
        let key = payout::idempotency_key(PayoutKind::Zkas, start, end);
        let cycle: PayoutCycle = match payout::find_cycle_by_idempotency_key(&self.db, &key).await? {
            Some(c) => c,
            None => {
                let plan = plan_zkas_cycle(
                    &self.db,
                    PlanZkasCycleParams {
                        daa_start: start,
                        daa_end: end,
                        threshold_sompi: self.cfg.threshold_sompi,
                        per_wallet_cap_sompi: self.cfg.per_wallet_cap_sompi,
                    },
                )
                .await?;
                stats.planned = plan.payouts_planned;
                plan.cycle
            }
        };

        // ---- broadcast pass ------------------------------------------
        self.broadcast_pass(&cycle, &mut stats).await?;

        // ---- confirmation pass (all zkas cycles, incl. stragglers) ---
        self.confirm_pass(virtual_daa, &mut stats).await?;

        // ---- reconcile cycle status ----------------------------------
        self.reconcile_cycle(cycle.id).await?;

        Ok(stats)
    }

    async fn inflight_latch(&self) -> Result<Option<String>, EngineError> {
        Ok(pool_meta::get(&self.db, INFLIGHT_KEY).await?.map(|e| e.value))
    }

    async fn broadcast_pass(&self, cycle: &PayoutCycle, stats: &mut TickStats) -> Result<(), EngineError> {
        if let Some(latched) = self.inflight_latch().await? {
            stats.inflight_latched = true;
            error!(
                instance = %self.cfg.instance_id,
                payout_id = %latched,
                "zkas payout: IN-FLIGHT LATCH SET — broadcasting halted until operator reconciles \
                 (verify whether the latched payout's tx landed, then clear pool_meta key `{}`)",
                INFLIGHT_KEY
            );
            return Ok(());
        }

        let recipients = payout::list_cycle_recipients(&self.db, cycle.id).await?;
        let planned: Vec<&CycleRecipient> =
            recipients.iter().filter(|r| r.status == PayoutStatus::Planned).collect();
        if planned.is_empty() {
            return Ok(());
        }

        let total: i64 = planned.iter().map(|r| r.amount_sompi).sum();
        if over_spend_cap(total, self.cfg.spend_cap_sompi) {
            stats.spend_capped = true;
            error!(
                instance = %self.cfg.instance_id,
                total,
                cap = ?self.cfg.spend_cap_sompi,
                "zkas payout: cycle total exceeds spend cap; nothing broadcast"
            );
            return Ok(());
        }

        for r in planned.into_iter().take(self.cfg.max_sends_per_tick) {
            if self.cfg.mode == ExecutionMode::DryRun {
                info!(
                    instance = %self.cfg.instance_id,
                    payout_id = r.payout_id,
                    to = %katpool_domain::redact::address(&r.address),
                    amount_sompi = r.amount_sompi,
                    "zkas payout DRY-RUN: would send"
                );
                continue;
            }

            stats.sends_attempted += 1;
            // Latch BEFORE the spend so a crash mid-send halts future
            // broadcasting instead of double-paying.
            pool_meta::set(&self.db, INFLIGHT_KEY, &r.payout_id.to_string()).await?;

            let Ok(amount) = u64::try_from(r.amount_sompi) else {
                error!(payout_id = r.payout_id, amount = r.amount_sompi, "negative payout amount; skipping");
                pool_meta::delete(&self.db, INFLIGHT_KEY).await?;
                continue;
            };
            match self.sender.send(&r.address, amount).await {
                Ok(txid) => {
                    payout::mark_payout_submitted(
                        &self.db,
                        r.payout_id,
                        katpool_domain::BlockHash::from_bytes(txid.as_bytes()),
                    )
                    .await?;
                    pool_meta::delete(&self.db, INFLIGHT_KEY).await?;
                    stats.sends_accepted += 1;
                }
                Err(e) if e.is_ambiguous() => {
                    // Timeout / unparsable txid: the tx may be in flight.
                    // Keep the latch; every future tick refuses to broadcast
                    // until the operator reconciles.
                    error!(
                        instance = %self.cfg.instance_id,
                        payout_id = r.payout_id,
                        error = %e,
                        "zkas payout: AMBIGUOUS send outcome — latch kept, broadcasting halted"
                    );
                    return Ok(());
                }
                Err(e) => {
                    // Provably-failed before submission (spawn error, seed
                    // error, CLI fatal): safe to clear and retry next tick.
                    warn!(
                        instance = %self.cfg.instance_id,
                        payout_id = r.payout_id,
                        error = %e,
                        "zkas payout: send failed pre-submission; will retry next tick"
                    );
                    pool_meta::delete(&self.db, INFLIGHT_KEY).await?;
                    // One failure usually means the node/wallet is unhappy —
                    // don't burn the remaining budget this tick.
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn ensure_confirm_cursor(&self) -> Result<(), EngineError> {
        if pool_meta::get(&self.db, CONFIRM_CURSOR_KEY).await?.is_none() {
            let sink = self.chain.sink().await?;
            pool_meta::set(&self.db, CONFIRM_CURSOR_KEY, &sink.to_string()).await?;
            info!(instance = %self.cfg.instance_id, cursor = %sink, "zkas payout: confirmation cursor anchored at sink");
        }
        Ok(())
    }

    async fn confirm_pass(&self, virtual_daa: u64, stats: &mut TickStats) -> Result<(), EngineError> {
        let in_flight = payout::list_zkas_in_flight_payouts(&self.db).await?;

        // Advance the acceptance cursor even with nothing in flight, so it
        // never falls far behind the sink.
        let cursor_row = pool_meta::get(&self.db, CONFIRM_CURSOR_KEY).await?;
        let Some(cursor_row) = cursor_row else {
            self.ensure_confirm_cursor().await?;
            return Ok(());
        };
        let Ok(cursor) = cursor_row.value.parse::<KaspaHash>() else {
            warn!(value = %cursor_row.value, "zkas payout: malformed confirm cursor; re-anchoring at sink");
            let sink = self.chain.sink().await?;
            pool_meta::set(&self.db, CONFIRM_CURSOR_KEY, &sink.to_string()).await?;
            return Ok(());
        };

        let watched: HashSet<KaspaHash> = in_flight
            .iter()
            .filter_map(|p| <[u8; 32]>::try_from(p.tx_hash.as_slice()).ok().map(KaspaHash::from_bytes))
            .collect();

        let scan = match self.chain.accepted_since(cursor, &watched).await {
            Ok(scan) => scan,
            Err(ChainError::CursorInvalid) => {
                // Deep reorg past the cursor. Re-anchor at the sink; any
                // acceptance missed in the gap leaves rows `submitted`
                // (Unknown) for operator reconciliation — never auto-failed.
                let sink = self.chain.sink().await?;
                warn!(stale = %cursor, reset = %sink, "zkas payout: confirm cursor reorged; re-anchored (in-flight rows may need manual review)");
                pool_meta::set(&self.db, CONFIRM_CURSOR_KEY, &sink.to_string()).await?;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        for p in &in_flight {
            let Ok(bytes) = <[u8; 32]>::try_from(p.tx_hash.as_slice()) else { continue };
            let txid = KaspaHash::from_bytes(bytes);
            let newly_accepted_daa = scan.accepted.get(&txid).copied();
            let recorded = p.accepted_daa_score.and_then(|v| u64::try_from(v).ok());
            let accept_daa = recorded.or(newly_accepted_daa);

            // Only pay the mempool probe for rows with no acceptance signal.
            let in_mempool = if accept_daa.is_none() { self.chain.in_mempool(txid).await? } else { false };

            match classify_confirmation(
                ConfirmationInputs { virtual_daa_score: virtual_daa, in_mempool, accept_daa },
                ZKAS_PAYOUT_CONFIRMATION_DAA,
            ) {
                ConfirmationState::Confirmed => {
                    if let (Some(daa), None) = (newly_accepted_daa, recorded) {
                        payout::mark_payout_accepted(&self.db, p.payout_id, daa as i64).await?;
                    }
                    payout::mark_payout_confirmed(&self.db, p.payout_id).await?;
                    stats.confirmed += 1;
                }
                ConfirmationState::Accepted => {
                    if recorded.is_none()
                        && let Some(daa) = newly_accepted_daa
                    {
                        payout::mark_payout_accepted(&self.db, p.payout_id, daa as i64).await?;
                        stats.accepted += 1;
                    }
                }
                // Pending / Unknown: no state change (money-safety posture).
                ConfirmationState::Pending | ConfirmationState::Unknown => {}
            }
        }

        if let Some(new_cursor) = scan.new_cursor {
            pool_meta::set(&self.db, CONFIRM_CURSOR_KEY, &new_cursor.to_string()).await?;
        }
        Ok(())
    }

    /// Fold per-recipient statuses into the cycle status.
    async fn reconcile_cycle(&self, cycle_id: i64) -> Result<(), EngineError> {
        let rows = payout::list_for_cycle(&self.db, cycle_id).await?;
        if rows.is_empty() {
            return Ok(());
        }
        let confirmed = rows.iter().filter(|p| p.status == PayoutStatus::Confirmed).count();
        let failed = rows.iter().filter(|p| p.status == PayoutStatus::Failed).count();
        let in_flight = rows
            .iter()
            .filter(|p| matches!(p.status, PayoutStatus::Submitted | PayoutStatus::Accepted))
            .count();

        let status = if confirmed == rows.len() {
            Some(PayoutCycleStatus::Settled)
        } else if confirmed > 0 {
            Some(PayoutCycleStatus::PartiallySettled)
        } else if in_flight > 0 {
            Some(PayoutCycleStatus::Broadcasting)
        } else if failed == rows.len() {
            Some(PayoutCycleStatus::Failed)
        } else {
            None // still planned
        };
        match status {
            Some(PayoutCycleStatus::Settled) => payout::mark_cycle_settled(&self.db, cycle_id).await?,
            Some(PayoutCycleStatus::PartiallySettled) => {
                payout::mark_cycle_partially_settled(&self.db, cycle_id).await?;
            }
            Some(PayoutCycleStatus::Broadcasting) => {
                payout::mark_cycle_broadcasting(&self.db, cycle_id).await?;
            }
            Some(PayoutCycleStatus::Failed) => payout::mark_cycle_failed(&self.db, cycle_id).await?,
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::over_spend_cap;

    #[test]
    fn spend_cap_disabled_when_none() {
        assert!(!over_spend_cap(i64::MAX, None));
    }

    #[test]
    fn spend_cap_trips_above_only() {
        assert!(!over_spend_cap(100, Some(100)));
        assert!(over_spend_cap(101, Some(100)));
    }
}
