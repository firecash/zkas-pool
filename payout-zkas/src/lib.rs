//! # payout-zkas — the ZKas shielded payout engine
//!
//! Pays each eligible miner's **full accrued PROP balance** from the pool's
//! Orchard shielded treasury: no vesting, no claim, no signature — a
//! scheduled sweep sends one shielded transaction per recipient to the
//! address they mined with, once their balance clears the dust threshold.
//!
//! Replaces `payout-kas`/`payout-krc20` on ZKas, where the treasury holds
//! shielded notes instead of transparent UTXOs:
//!
//! - **planning** reuses the shared `payout_cycle`/`payout` tables with
//!   `kind = 'zkas'` ([`plan_zkas_cycle`]);
//! - **spending** shells out to the live-verified `shielded-pay` CLI
//!   ([`ShieldedPayCli`]) — one Orchard proof per recipient, serial,
//!   bounded per tick;
//! - **confirmation** reads the virtual chain's accepted-transaction ids
//!   (no transparent change coin exists to watch) and settles at
//!   [`ZKAS_PAYOUT_CONFIRMATION_DAA`] depth;
//! - **money safety**: single-leader advisory lock, per-cycle spend cap,
//!   and an in-flight latch that halts all broadcasting on any ambiguous
//!   send outcome (see [`engine`] docs for the reconcile runbook).

pub mod chain;
pub mod confirm;
pub mod engine;
pub mod plan;
pub mod sender;
pub mod window;

pub use chain::{AcceptanceScan, ChainError, ChainReader, GrpcChainReader};
pub use confirm::{
    ConfirmationInputs, ConfirmationState, ZKAS_PAYOUT_CONFIRMATION_DAA, classify_confirmation,
};
pub use engine::{
    CONFIRM_CURSOR_KEY, EngineError, ExecutionMode, INFLIGHT_KEY, TickStats, ZkasPayoutEngine,
    ZkasPayoutEngineConfig, over_spend_cap,
};
pub use plan::{
    DEFAULT_PER_WALLET_CAP_SOMPI, PlanZkasCycleParams, PlanZkasCycleResult, plan_zkas_cycle,
};
pub use sender::{
    DEFAULT_PAYOUT_FEE_SOMPI, DEFAULT_SEND_TIMEOUT, SendError, ShieldedPayCli, ShieldedSender,
    parse_txid,
};
pub use window::cycle_window;
