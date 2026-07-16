//! Pool accountant.
//!
//! Subscribes to the bridge's `PoolEvent` broadcast channel and
//! mirrors share + block lifecycle events into the new schema.
//! Holds the pool's fee model (operator-tunable topline fee +
//! tier-aware NACHO rebate) and the wallet-tier classifier — both
//! of which are scaffolded in this milestone (M1) but only
//! exercised end-to-end by the M3 allocation engine.
//!
//! ## Architecture
//!
//! ```text
//!         ┌───────────────┐
//!         │  bridge       │ tokio::sync::broadcast::Sender<PoolEvent>
//!         └──────┬────────┘
//!                │
//!         ┌──────▼─────────────────────────┐
//!         │  accountant::EventConsumer     │ (this crate)
//!         │   • ShareCredited  → share     │
//!         │   • BlockFound     → block     │
//!         │   • BlockAccepted  → transition│
//!         └──────┬─────────────────────────┘
//!                │
//!         ┌──────▼────────┐
//!         │  katpool-db   │ wallet / worker / share / block repo
//!         └───────────────┘
//! ```
//!
//! ## Phase 3 milestone surface (this PR is M1)
//!
//! - **M1**: scaffold + share/block event ingestion +
//!   `FeeConfig` + `WalletTier` + `TierClassifier` trait with the
//!   `StaticTierClassifier` stub.
//! - **M2** (this PR): share-window aggregation primitive
//!   (`WindowAggregator`), share-reject persistence
//!   (`share_reject` table + repo), and the per-miner stats
//!   read-side primitives (`share_stats` repo).
//! - **M3**: PROP allocation engine + cached HTTP
//!   `KasplexTierClassifier` + schema migration adding
//!   `applied_topline_bps`, `applied_rebate_bps`, `applied_tier`
//!   to `share_allocation`.
//! - **M4**: 24h-production-log replay determinism harness (`accountant::replay`).
//!
//! See [`docs/decisions/0012-fee-model-and-tier-classification.md`](../../docs/decisions/0012-fee-model-and-tier-classification.md)
//! for the architectural decision record.

#![cfg_attr(not(test), warn(missing_docs))]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::float_arithmetic,
    )
)]

pub mod allocation;
pub mod config;
pub mod consumer;
pub mod error;
pub mod geoip;
pub mod kaspad_grpc;
pub mod maturity;
pub mod metrics;
pub mod payout;
pub mod replay;
pub mod shielded_scan;
pub mod tier;
pub mod tier_kasplex;
pub mod vesting;
pub mod window;

pub use allocation::{AllocationEngine, AllocationEngineError, AllocationOutcome};
pub use config::{
    Allocation, AllocationError, DEFAULT_TOPLINE_BPS, ELITE_REBATE_BPS, FeeConfig,
    STANDARD_REBATE_BPS, WalletTier,
};
pub use consumer::{ConsumerConfig, ConsumerConfigError, EventConsumer, VALID_NETWORKS};
pub use error::{AccountantError, EventError};
pub use payout::{
    has_auto_payable, plan_payout, time_until_auto_payout, PayoutPlan, PayoutTrigger,
};
pub use vesting::{
    ClaimTotals, EARLY_PAYOUT_BPS, FULL_PAYOUT_BPS, ForfeitPolicy, VESTING_CLIFF,
    VESTING_CLIFF_DAYS, VestedSplit, vest_claim, vest_reward,
};
pub use geoip::{GeoIp, GeoIpError};
pub use kaspad_grpc::{KaspadGrpcClient, coinbase_utxos_from_entries};
pub use maturity::{
    BlockColor, CoinbaseUtxo, DEFAULT_BATCH_SIZE, DEFAULT_COINBASE_MATURITY, DEFAULT_POLL_INTERVAL,
    DEFAULT_WINDOW_DAA_SPAN, KaspadClient, KaspadError, MaturityConfig, MaturityTracker,
    SweepStats, TrackerError, is_mature,
};
pub use replay::{
    DbSnapshot, assert_snapshots_equal, load_ndjson_path, load_ndjson_reader, replay_all, snapshot,
    verify_dual_replay,
};
pub use shielded_scan::{
    DEFAULT_MATURITY_SAFETY_DAA, DEFAULT_PAGE_LIMIT, SHIELDED_SCAN_CURSOR_KEY,
    ScannerConfigError, ShieldedRewardScanner, extract_treasury_rewards,
};
pub use tier::{ClassifierError, StaticTierClassifier, TierClassifier};
pub use tier_kasplex::{
    DEFAULT_ELITE_KRC20_THRESHOLD, DEFAULT_HTTP_TIMEOUT, DEFAULT_KRC20_BASE, DEFAULT_NACHO_TICKER,
    DEFAULT_NFT_BASE, DEFAULT_TTL, KasplexConfig, KasplexTierClassifier,
};
pub use window::{CloseOutcome, WindowAggregator};

/// Crate version constant.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
