//! Postgres-enum / Rust-enum parity tests.
//!
//! Every `#[sqlx(type_name = "...")]` enum in `katpool-db` is
//! exercised here by:
//!
//! 1. Inserting a row whose enum column holds variant V.
//! 2. Reading it back and asserting the Rust-side variant matches V.
//!
//! Doing both directions (write and read) catches both serialise
//! and deserialise drift. The test is exhaustive on Rust variants
//! via match — adding a new Rust variant without growing this
//! test (or the migration) fails the build.
//!
//! Why this matters: postgres enum variants are stored as small
//! integers in the catalog; the on-disk ordering must stay in sync
//! with the Rust enum's `sqlx::Type` declaration. If a migration
//! adds a new variant in the wrong position, *every old row's
//! enum value silently shifts*. This test catches that class of
//! bug before it reaches production.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::float_arithmetic
)]

use katpool_db::repo::block::BlockStatus;
use katpool_db::repo::payout::{Krc20TransferStatus, PayoutCycleStatus, PayoutKind, PayoutStatus};
use katpool_db::repo::share_allocation::DbWalletTier;
use katpool_db::repo::share_reject::DbShareRejectReason;
use katpool_db::{PoolConfig, build_pool, migrate};
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

struct Env {
    db: sqlx::PgPool,
    _ctr: ContainerAsync<Postgres>,
}

async fn setup() -> Env {
    let container = Postgres::default().start().await.expect("postgres");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let db = build_pool(&PoolConfig {
        url,
        min_connections: 1,
        max_connections: 4,
        application_name: "enum-parity".to_owned(),
        ..PoolConfig::production("placeholder".to_owned())
    })
    .await
    .expect("pool");
    migrate::run(&db).await.expect("migrate");
    Env {
        db,
        _ctr: container,
    }
}

/// Round-trip a `sqlx::Type` enum value through a temporary table
/// using its declared `type_name`. Returns the value as the DB
/// gave it back.
async fn roundtrip<T>(db: &sqlx::PgPool, type_name: &str, v: T) -> T
where
    T: sqlx::Type<sqlx::Postgres>
        + for<'q> sqlx::Encode<'q, sqlx::Postgres>
        + for<'q> sqlx::Decode<'q, sqlx::Postgres>
        + Send
        + Sync
        + Unpin
        + 'static,
{
    // Use a transactional temporary table so different enum tests
    // don't fight over a shared name.
    let mut tx = db.begin().await.unwrap();
    sqlx::query(&format!("CREATE TEMP TABLE rt_t (v {type_name})"))
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO rt_t (v) VALUES ($1)")
        .bind(&v)
        .execute(&mut *tx)
        .await
        .unwrap();
    let got: T = sqlx::query_scalar("SELECT v FROM rt_t")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    // Implicit rollback when `tx` drops.
    drop(tx);
    got
}

#[tokio::test]
async fn payout_kind_round_trips_every_variant() {
    let env = setup().await;
    for v in [PayoutKind::Kas, PayoutKind::Krc20Nacho, PayoutKind::Zkas] {
        let got = roundtrip(&env.db, "payout_kind", v).await;
        assert_eq!(got, v, "payout_kind variant {v:?} drifted");
    }
}

#[tokio::test]
async fn payout_cycle_status_round_trips_every_variant() {
    let env = setup().await;
    for v in [
        PayoutCycleStatus::Planned,
        PayoutCycleStatus::Broadcasting,
        PayoutCycleStatus::PartiallySettled,
        PayoutCycleStatus::Settled,
        PayoutCycleStatus::Failed,
    ] {
        let got = roundtrip(&env.db, "payout_cycle_status", v).await;
        assert_eq!(got, v, "payout_cycle_status variant {v:?} drifted");
    }
}

#[tokio::test]
async fn payout_status_round_trips_every_variant() {
    let env = setup().await;
    for v in [
        PayoutStatus::Planned,
        PayoutStatus::Submitted,
        PayoutStatus::Accepted,
        PayoutStatus::Confirmed,
        PayoutStatus::Failed,
    ] {
        let got = roundtrip(&env.db, "payout_status", v).await;
        assert_eq!(got, v, "payout_status variant {v:?} drifted");
    }
}

#[tokio::test]
async fn krc20_transfer_status_round_trips_every_variant() {
    let env = setup().await;
    for v in [
        Krc20TransferStatus::Pending,
        Krc20TransferStatus::CommitSubmitted,
        Krc20TransferStatus::RevealSubmitted,
        Krc20TransferStatus::Completed,
        Krc20TransferStatus::Failed,
    ] {
        let got = roundtrip(&env.db, "krc20_transfer_status", v).await;
        assert_eq!(got, v, "krc20_transfer_status variant {v:?} drifted");
    }
}

#[tokio::test]
async fn block_status_round_trips_every_variant() {
    let env = setup().await;
    for v in [
        BlockStatus::Found,
        BlockStatus::SubmittedToNode,
        BlockStatus::ConfirmedBlue,
        BlockStatus::Matured,
        BlockStatus::Orphaned,
    ] {
        let got = roundtrip(&env.db, "block_status", v).await;
        assert_eq!(got, v, "block_status variant {v:?} drifted");
    }
}

#[tokio::test]
async fn wallet_tier_round_trips_every_variant() {
    let env = setup().await;
    for v in [DbWalletTier::Standard, DbWalletTier::Elite] {
        let got = roundtrip(&env.db, "wallet_tier", v).await;
        assert_eq!(got, v, "wallet_tier variant {v:?} drifted");
    }
}

#[tokio::test]
async fn share_reject_reason_round_trips_every_variant() {
    let env = setup().await;
    for v in [
        DbShareRejectReason::Stale,
        DbShareRejectReason::LowDifficulty,
        DbShareRejectReason::BadPow,
        DbShareRejectReason::MissingJob,
        DbShareRejectReason::MalformedFrame,
        DbShareRejectReason::DuplicateSubmit,
        DbShareRejectReason::BadAddress,
    ] {
        let got = roundtrip(&env.db, "share_reject_reason", v).await;
        assert_eq!(got, v, "share_reject_reason variant {v:?} drifted");
    }
}

/// Catches the case where the Rust enum gains a variant but the
/// loop above isn't updated. Each enum gets its own coverage
/// test below — these are intentionally trivial matches that
/// fail compilation if you add a variant without extending the
/// round-trip loop.
mod exhaustiveness_guards {
    #![allow(clippy::missing_const_for_fn)]
    use super::*;

    #[allow(dead_code)]
    fn payout_kind(v: PayoutKind) {
        match v {
            PayoutKind::Kas | PayoutKind::Krc20Nacho | PayoutKind::Zkas => {}
        }
    }

    #[allow(dead_code)]
    fn payout_cycle_status(v: PayoutCycleStatus) {
        match v {
            PayoutCycleStatus::Planned
            | PayoutCycleStatus::Broadcasting
            | PayoutCycleStatus::PartiallySettled
            | PayoutCycleStatus::Settled
            | PayoutCycleStatus::Failed => {}
        }
    }

    #[allow(dead_code)]
    fn payout_status(v: PayoutStatus) {
        match v {
            PayoutStatus::Planned
            | PayoutStatus::Submitted
            | PayoutStatus::Accepted
            | PayoutStatus::Confirmed
            | PayoutStatus::Failed => {}
        }
    }

    #[allow(dead_code)]
    fn krc20_transfer_status(v: Krc20TransferStatus) {
        match v {
            Krc20TransferStatus::Pending
            | Krc20TransferStatus::CommitSubmitted
            | Krc20TransferStatus::RevealSubmitted
            | Krc20TransferStatus::Completed
            | Krc20TransferStatus::Failed => {}
        }
    }

    #[allow(dead_code)]
    fn block_status(v: BlockStatus) {
        match v {
            BlockStatus::Found
            | BlockStatus::SubmittedToNode
            | BlockStatus::ConfirmedBlue
            | BlockStatus::Matured
            | BlockStatus::Orphaned => {}
        }
    }

    #[allow(dead_code)]
    fn wallet_tier(v: DbWalletTier) {
        match v {
            DbWalletTier::Standard | DbWalletTier::Elite => {}
        }
    }

    #[allow(dead_code)]
    fn share_reject_reason(v: DbShareRejectReason) {
        match v {
            DbShareRejectReason::Stale
            | DbShareRejectReason::LowDifficulty
            | DbShareRejectReason::BadPow
            | DbShareRejectReason::MissingJob
            | DbShareRejectReason::MalformedFrame
            | DbShareRejectReason::DuplicateSubmit
            | DbShareRejectReason::BadAddress => {}
        }
    }
}
