//! Block maturity tracker.
//!
//! Drives two independent, idempotent concerns each sweep:
//!
//! 1. **Block lifecycle telemetry.** For every `submitted_to_node`
//!    block, ask kaspad for its GHOSTDAG colour and transition it to a
//!    terminal state: `confirmed_blue` (accepted blue → the pool earned
//!    its reward) or `orphaned` (merged red, or never merged after the
//!    coinbase-maturity depth → no reward). This is operator-facing
//!    telemetry only; it does **not** drive money.
//!
//! 2. **Coinbase-reward allocation.** Scan the coinbase UTXOs credited
//!    to the pool address. Each one that has reached consensus
//!    coinbase-maturity (`virtual_daa_score ≥ utxo.block_daa_score +
//!    coinbase_maturity`) is recorded in `coinbase_reward` (idempotent
//!    by outpoint) and handed to the [`AllocationEngine`] for PROP
//!    distribution over the DAA window ending at the UTXO's
//!    `block_daa_score`. The UTXO set is the ground truth for realised
//!    reward, so DAG re-orgs need no special handling: a reward that
//!    re-orgs out before maturity simply never appears.
//!
//! ## Why colour, not `is_chain_block`
//!
//! A block's reward is paid only if it is **blue** (in some chain
//! block's blue mergeset), which `RpcApi::get_current_block_color`
//! reports directly. `is_chain_block` (selected-parent-chain
//! membership) is a far narrower set and is *not* the reward
//! condition — using it strands almost every rewarded block. See
//! ADR-0014.
//!
//! ## Why the UTXO set, not the block's own coinbase
//!
//! A block B's own coinbase pays the miners of the blocks **B merges**,
//! not B itself; B's reward is paid in the coinbase of the later chain
//! block that merges B. The only exact, attribution-free source of "the
//! pool was paid N sompi" is the coinbase UTXO credited to the pool
//! address. See ADR-0014.
//!
//! kaspad access goes through the [`KaspadClient`] trait so the
//! sweep logic is unit-testable against an in-memory fake.

#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use katpool_db::DbError;
use katpool_db::repo::block::{self, Block, BlockStatus, EnsureOutcome};
use katpool_db::repo::coinbase_reward::{self, CoinbaseReward};
use katpool_domain::{BlockHash, DaaScore};
use sqlx::PgPool;
use tokio::sync::watch;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::allocation::{AllocationEngine, AllocationEngineError, AllocationOutcome};

/// Default polling interval between sweeps.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(15);

/// Default coinbase maturity in DAA-score depth.
///
/// Matches the ZKas mainnet consensus parameter:
/// `BlockrateParams::coinbase_maturity = BPS(1) × COINBASE_MATURITY_SECONDS(100) = 100`
/// (the chain runs at 1 block/second — see `consensus/core/src/config/bps.rs`
/// in the rusty-kaspa fork). A coinbase reward is spendable when
/// `virtual_daa_score ≥ reward.block_daa_score + coinbase_maturity`.
/// Upstream Kaspa mainnet/testnet-10 (10 BPS) would be 1000.
pub const DEFAULT_COINBASE_MATURITY: u64 = 100;

/// Default DAA-window span for PROP allocation, in DAA scores.
/// 600 DAA ≈ 10 minutes at ZKas's 1 BPS — a trailing window long enough
/// to smooth share variance across many block rewards. See ADR-0014.
pub const DEFAULT_WINDOW_DAA_SPAN: u64 = 600;

/// Default per-sweep batch limit (block transitions + reward
/// allocations). Bounds tail latency against a pathological backlog.
pub const DEFAULT_BATCH_SIZE: i64 = 200;

/// Errors surfaced by the [`KaspadClient`] trait.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KaspadError {
    /// Transport-level failure (gRPC channel down, timeout, etc.).
    #[error("kaspad transport error: {0}")]
    Transport(String),

    /// kaspad responded with a payload the client couldn't parse.
    #[error("kaspad payload malformed: {0}")]
    Malformed(String),
}

/// GHOSTDAG colour of a block relative to the current virtual chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockColor {
    /// Merged as blue by a chain block → the pool earned its reward.
    Blue,
    /// Merged as red → no reward.
    Red,
    /// Not yet merged into the sink's past (kaspad returns
    /// `MergerNotFound`). Includes hashes kaspad has not seen.
    NotYetMerged,
}

/// One coinbase UTXO credited to the pool address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoinbaseUtxo {
    /// 32-byte coinbase transaction id of the outpoint.
    pub transaction_id: [u8; 32],
    /// Output index within the coinbase transaction.
    pub index: u32,
    /// UTXO value in sompi.
    pub amount_sompi: u64,
    /// DAA score of the block that created the UTXO (the accepting
    /// chain block). Drives both the maturity gate and the PROP window.
    pub block_daa_score: u64,
}

/// Minimal kaspad surface the maturity tracker needs.
#[async_trait]
pub trait KaspadClient: Send + Sync + 'static {
    /// Current virtual DAA score (used for the coinbase-maturity gate).
    async fn get_virtual_daa_score(&self) -> Result<u64, KaspadError>;

    /// GHOSTDAG colour of one block relative to the current sink.
    async fn get_block_color(&self, hash: BlockHash) -> Result<BlockColor, KaspadError>;

    /// Coinbase UTXOs currently credited to the pool address(es).
    /// Non-coinbase UTXOs are filtered out by the implementation.
    async fn get_pool_coinbase_utxos(&self) -> Result<Vec<CoinbaseUtxo>, KaspadError>;
}

/// Runtime knobs for the tracker.
#[derive(Debug, Clone, Copy)]
pub struct MaturityConfig {
    /// Sweep cadence. Defaults to 15 s.
    pub poll_interval: Duration,
    /// Coinbase maturity in DAA-score depth (consensus parameter).
    pub coinbase_maturity: u64,
    /// DAA-score span of the PROP window that ends at the matured
    /// reward's `block_daa_score`.
    pub window_daa_span: u64,
    /// Max block transitions and reward allocations per sweep.
    pub batch_size: i64,
    /// Cutover DAA-score floor. Coinbase UTXOs whose `block_daa_score` is
    /// **below** this are ignored entirely — never recorded in
    /// `coinbase_reward`, never allocated. `0` (the default) disables the
    /// floor. Set it to the cutover DAA score when this pool adopts a treasury
    /// address that a prior pool already mined to, so the prior pool's
    /// historical coinbases are not re-discovered and retained as no-wallet
    /// rewards (they were already paid out by the prior pool).
    pub coinbase_min_daa_score: u64,
}

impl Default for MaturityConfig {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            coinbase_maturity: DEFAULT_COINBASE_MATURITY,
            window_daa_span: DEFAULT_WINDOW_DAA_SPAN,
            batch_size: DEFAULT_BATCH_SIZE,
            coinbase_min_daa_score: 0,
        }
    }
}

/// Outcome counters for one sweep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepStats {
    /// Blocks transitioned `submitted_to_node → confirmed_blue`.
    pub confirmed_blue: u64,
    /// Blocks transitioned to `orphaned` (red, or never merged within
    /// the maturity depth).
    pub orphaned: u64,
    /// `submitted_to_node` blocks not yet resolvable (not yet merged
    /// and still within the maturity depth).
    pub blocks_waiting: u64,
    /// Matured coinbase UTXOs newly recorded in `coinbase_reward`.
    pub rewards_discovered: u64,
    /// Coinbase rewards allocated to contributing wallets this sweep.
    pub rewards_allocated: u64,
    /// Coinbase rewards finalised with no contributing wallets
    /// (retained by the pool).
    pub rewards_empty: u64,
    /// Per-item errors that didn't kill the sweep.
    pub errors: u64,
    /// Coinbase UTXOs ignored because they predate the cutover DAA floor
    /// (`coinbase_min_daa_score`). Always 0 when no floor is configured.
    pub rewards_skipped_below_floor: u64,
}

/// The tracker.
pub struct MaturityTracker {
    db: PgPool,
    kaspad: Arc<dyn KaspadClient>,
    engine: Arc<AllocationEngine>,
    cfg: MaturityConfig,
    instance_id: String,
    /// Optional kaspad-reachability signal. Each [`Self::run_loop`] sweep
    /// publishes `true` on a successful poll and `false` on a failed one,
    /// letting an embedder (the unified runtime's API readiness probe) reuse
    /// this poller instead of opening a second kaspad connection.
    sync_observer: Option<watch::Sender<bool>>,
}

impl MaturityTracker {
    /// Construct a tracker.
    #[must_use]
    pub const fn new(
        db: PgPool,
        kaspad: Arc<dyn KaspadClient>,
        engine: Arc<AllocationEngine>,
        cfg: MaturityConfig,
        instance_id: String,
    ) -> Self {
        Self {
            db,
            kaspad,
            engine,
            cfg,
            instance_id,
            sync_observer: None,
        }
    }

    /// Attach a kaspad-reachability observer published once per sweep.
    ///
    /// Consumed builder-style at wiring time. The sender's initial `watch`
    /// value is whatever the caller seeded the channel with; the first sweep
    /// overwrites it. Dropped silently if no sweep ever runs.
    #[must_use]
    pub fn with_sync_observer(mut self, observer: watch::Sender<bool>) -> Self {
        self.sync_observer = Some(observer);
        self
    }

    /// One sweep: block-lifecycle telemetry, then coinbase-reward
    /// allocation. Public for tests; production wiring uses
    /// [`Self::run_loop`].
    #[allow(clippy::cognitive_complexity)]
    pub async fn run_once(&self) -> Result<SweepStats, TrackerError> {
        let virtual_daa_score = self
            .kaspad
            .get_virtual_daa_score()
            .await
            .map_err(TrackerError::Kaspad)?;
        debug!(instance = %self.instance_id, virtual_daa_score, "tracker sweep start");

        let mut stats = SweepStats::default();
        self.sweep_blocks(virtual_daa_score, &mut stats).await?;
        self.sweep_rewards(virtual_daa_score, &mut stats).await?;

        info!(
            instance = %self.instance_id,
            virtual_daa_score,
            ?stats,
            "tracker sweep done"
        );
        Ok(stats)
    }

    /// Resolve `submitted_to_node` blocks to a terminal lifecycle state.
    async fn sweep_blocks(
        &self,
        virtual_daa_score: u64,
        stats: &mut SweepStats,
    ) -> Result<(), TrackerError> {
        let active = block::list_by_status(
            &self.db,
            &[BlockStatus::SubmittedToNode],
            self.cfg.batch_size,
        )
        .await
        .map_err(TrackerError::Db)?;

        for blk in active {
            match self.process_submitted(&blk, virtual_daa_score).await {
                Ok(BlockOutcome::ConfirmedBlue) => stats.confirmed_blue += 1,
                Ok(BlockOutcome::Orphaned) => stats.orphaned += 1,
                Ok(BlockOutcome::StillWaiting) => stats.blocks_waiting += 1,
                Err(e) => {
                    stats.errors += 1;
                    let hash_hex = hex::encode(&blk.hash);
                    error!(
                        instance = %self.instance_id,
                        hash = %hash_hex,
                        error = %e,
                        "tracker per-block error; continuing sweep"
                    );
                }
            }
        }
        Ok(())
    }

    async fn process_submitted(
        &self,
        blk: &Block,
        virtual_daa_score: u64,
    ) -> Result<BlockOutcome, TrackerError> {
        let hash = bytes_to_hash(&blk.hash).ok_or(TrackerError::Malformed {
            reason: "block.hash is not 32 bytes",
        })?;

        match self
            .kaspad
            .get_block_color(hash)
            .await
            .map_err(TrackerError::Kaspad)?
        {
            BlockColor::Blue => {
                block::mark_confirmed_blue(&self.db, hash, None)
                    .await
                    .map_err(TrackerError::Db)?;
                info!(instance = %self.instance_id, hash = %hash, "block confirmed blue");
                Ok(BlockOutcome::ConfirmedBlue)
            }
            BlockColor::Red => {
                block::mark_orphaned(&self.db, hash)
                    .await
                    .map_err(TrackerError::Db)?;
                warn!(instance = %self.instance_id, hash = %hash, "block orphaned (merged red)");
                Ok(BlockOutcome::Orphaned)
            }
            BlockColor::NotYetMerged => {
                // Age out blocks that never merged within the maturity
                // depth: an honest block is merged (blue or red) well
                // before this, so a block still unmerged this deep is
                // lost. Without this, a permanently-unmerged block would
                // sit in `submitted_to_node` forever.
                let block_daa = blk.daa_score as u64;
                if virtual_daa_score >= block_daa.saturating_add(self.cfg.coinbase_maturity) {
                    block::mark_orphaned(&self.db, hash)
                        .await
                        .map_err(TrackerError::Db)?;
                    warn!(
                        instance = %self.instance_id,
                        hash = %hash,
                        block_daa,
                        virtual_daa_score,
                        "block orphaned (never merged within maturity depth)"
                    );
                    Ok(BlockOutcome::Orphaned)
                } else {
                    Ok(BlockOutcome::StillWaiting)
                }
            }
        }
    }

    /// Discover matured coinbase UTXOs and allocate any not yet done.
    async fn sweep_rewards(
        &self,
        virtual_daa_score: u64,
        stats: &mut SweepStats,
    ) -> Result<(), TrackerError> {
        let utxos = self
            .kaspad
            .get_pool_coinbase_utxos()
            .await
            .map_err(TrackerError::Kaspad)?;

        // Record every matured UTXO (idempotent by outpoint).
        for utxo in &utxos {
            if !is_mature(
                virtual_daa_score,
                utxo.block_daa_score,
                self.cfg.coinbase_maturity,
            ) {
                continue;
            }
            // Cutover DAA floor: a coinbase UTXO from before the floor predates
            // this pool's takeover of the treasury address (e.g. the legacy
            // pool's historical blocks). Ignore it entirely so it is never
            // recorded or allocated — it was already paid out by the prior pool.
            if utxo.block_daa_score < self.cfg.coinbase_min_daa_score {
                stats.rewards_skipped_below_floor += 1;
                continue;
            }
            let Ok(amount) = i64::try_from(utxo.amount_sompi) else {
                stats.errors += 1;
                error!(
                    instance = %self.instance_id,
                    amount = utxo.amount_sompi,
                    "coinbase UTXO amount exceeds i64 range; skipping"
                );
                continue;
            };
            match coinbase_reward::ensure(
                &self.db,
                &utxo.transaction_id,
                utxo.index,
                amount,
                utxo.block_daa_score,
            )
            .await
            {
                Ok((_, EnsureOutcome::Inserted)) => stats.rewards_discovered += 1,
                Ok((_, EnsureOutcome::AlreadyExisted)) => {}
                Err(e) => {
                    stats.errors += 1;
                    error!(
                        instance = %self.instance_id,
                        error = %e,
                        "recording coinbase reward failed; continuing sweep"
                    );
                }
            }
        }

        // Allocate any unallocated rewards (all recorded rewards are
        // matured by construction).
        let pending = coinbase_reward::list_unallocated(&self.db, self.cfg.batch_size)
            .await
            .map_err(TrackerError::Db)?;
        for reward in pending {
            match self.allocate_reward(&reward).await {
                Ok(AllocationOutcome::Allocated { .. }) => stats.rewards_allocated += 1,
                Ok(AllocationOutcome::NoContributingWallets { .. }) => stats.rewards_empty += 1,
                Ok(AllocationOutcome::AlreadyAllocated) => {}
                Err(e) => {
                    stats.errors += 1;
                    error!(
                        instance = %self.instance_id,
                        reward_id = reward.id.0,
                        error = %e,
                        "allocating coinbase reward failed; continuing sweep"
                    );
                }
            }
        }
        Ok(())
    }

    async fn allocate_reward(
        &self,
        reward: &CoinbaseReward,
    ) -> Result<AllocationOutcome, TrackerError> {
        let daa = reward.block_daa_score as u64;
        let daa_end = DaaScore::new(daa);
        let daa_start = DaaScore::new(daa.saturating_sub(self.cfg.window_daa_span));
        self.engine
            .allocate_coinbase_reward(reward.id, reward.amount_sompi, daa_start, daa_end)
            .await
            .map_err(TrackerError::Engine)
    }

    /// Run the sweep on a fixed interval until `shutdown` fires.
    /// Designed to be `tokio::spawn`-ed.
    pub async fn run_loop(self, mut shutdown: watch::Receiver<bool>) -> Result<(), TrackerError> {
        let mut interval = time::interval(self.cfg.poll_interval);
        // Skip the immediate tick so the first sweep happens after
        // poll_interval (lets the consumer warm up).
        interval.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(instance = %self.instance_id, "tracker shutdown requested; exiting");
                        return Ok(());
                    }
                }
                _ = interval.tick() => {
                    let reachable = match self.run_once().await {
                        Ok(_) => true,
                        Err(e) => {
                            // A whole-sweep error (e.g. kaspad transport
                            // down) is logged but doesn't kill the loop —
                            // the next interval tick retries.
                            warn!(instance = %self.instance_id, error = %e, "tracker sweep failed; will retry");
                            false
                        }
                    };
                    // Publish kaspad reachability for any embedded readiness
                    // probe. `send` errors only when every receiver is gone,
                    // which is benign here.
                    if let Some(observer) = &self.sync_observer {
                        let _ = observer.send(reachable);
                    }
                }
            }
        }
    }
}

/// Whether a coinbase UTXO has reached consensus coinbase-maturity.
#[must_use]
pub const fn is_mature(
    virtual_daa_score: u64,
    block_daa_score: u64,
    coinbase_maturity: u64,
) -> bool {
    // virtual_daa_score >= block_daa_score + coinbase_maturity, written
    // to avoid overflow on adversarial inputs.
    match virtual_daa_score.checked_sub(block_daa_score) {
        Some(depth) => depth >= coinbase_maturity,
        None => false,
    }
}

#[derive(Debug)]
enum BlockOutcome {
    ConfirmedBlue,
    Orphaned,
    StillWaiting,
}

/// Top-level tracker errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TrackerError {
    /// kaspad upstream call failed.
    #[error("kaspad: {0}")]
    Kaspad(KaspadError),
    /// Database error.
    #[error("db: {0}")]
    Db(DbError),
    /// `AllocationEngine` failed mid-cycle.
    #[error("allocation engine: {0}")]
    Engine(AllocationEngineError),
    /// Schema-level invariant we couldn't recover from.
    #[error("malformed schema row: {reason}")]
    Malformed {
        /// Human-readable reason.
        reason: &'static str,
    },
}

const fn bytes_to_hash(bytes: &[u8]) -> Option<BlockHash> {
    // `block.hash` is exactly 32 bytes (schema CHECK), but guard
    // defensively. `first_chunk` avoids slice indexing in const.
    if bytes.len() != 32 {
        return None;
    }
    match bytes.first_chunk::<32>() {
        Some(arr) => Some(BlockHash::from_bytes(*arr)),
        None => None,
    }
}
