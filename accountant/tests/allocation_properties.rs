//! Property tests for `FeeConfig::compute_allocation`.
//!
//! Every property is universal over the validated input space —
//! any `(gross, topline_bps, tier)` triple in range must satisfy
//! each one. These tests are the strongest deterministic
//! verification we have for the pool's money math, because they
//! exercise the code at thousands of points proptest picks itself
//! rather than the handful a human test-writer would think of.
//!
//! Run with default `proptest` cases (256 per property); on a
//! laptop CI runner the file finishes in <1s, so we don't tune
//! the case count down.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::float_arithmetic,
    clippy::similar_names,
    // The test computes its own hand-rolled expected value using
    // the same integer-truncation math the production code uses.
    clippy::integer_division
)]

use accountant::{Allocation, AllocationError, FeeConfig, WalletTier};
use proptest::prelude::*;

/// Maximum gross we run proptest against. The
/// `gross * topline_bps` multiplication is exactly safe up to
/// `i64::MAX / 10_000 ≈ 9.22 × 10^14`; we round down to `10^14`
/// (= 1 000 000 KAS in sompi) for a clean number. That's
/// ~5 orders of magnitude above any realistic single-wallet
/// block-reward share. Allocations larger than this would
/// surface as `AllocationError::Overflow` from the production
/// path — exercised by a dedicated boundary test rather than
/// the property suite.
const MAX_GROSS: i64 = 100_000_000_000_000;

/// Strategy producing any valid `FeeConfig` (topline ∈ [0, 1000]).
fn fee_config_strategy() -> impl Strategy<Value = FeeConfig> {
    (0_u16..=1_000).prop_map(|bps| FeeConfig::new(bps).expect("bps validated"))
}

/// Strategy producing either tier with equal weight.
fn tier_strategy() -> impl Strategy<Value = WalletTier> {
    prop_oneof![Just(WalletTier::Standard), Just(WalletTier::Elite)]
}

/// Strategy producing a non-negative gross sompi value up to
/// `MAX_GROSS`.
fn gross_strategy() -> impl Strategy<Value = i64> {
    0_i64..=MAX_GROSS
}

proptest! {
    // ---- Core invariants ----------------------------------------

    /// The balance equation `gross == pool_fee + nacho + net`
    /// holds for every valid input. This is the schema's
    /// `share_allocation_balance` CHECK lifted into Rust.
    #[test]
    fn prop_balance_equation_holds(
        gross in gross_strategy(),
        cfg in fee_config_strategy(),
        tier in tier_strategy(),
    ) {
        let a = cfg.compute_allocation(gross, tier).expect("validated inputs");
        prop_assert_eq!(
            a.gross_sompi,
            a.pool_fee_sompi + a.nacho_accrual_sompi + a.net_payout_sompi
        );
        prop_assert!(a.is_balanced());
    }

    /// Every output component is non-negative.
    #[test]
    fn prop_components_non_negative(
        gross in gross_strategy(),
        cfg in fee_config_strategy(),
        tier in tier_strategy(),
    ) {
        let a = cfg.compute_allocation(gross, tier).expect("validated inputs");
        prop_assert!(a.pool_fee_sompi >= 0);
        prop_assert!(a.nacho_accrual_sompi >= 0);
        prop_assert!(a.net_payout_sompi >= 0);
    }

    /// At identical `(gross, cfg)`, elite must receive ≥ standard
    /// in NACHO accrual (its rebate ratio is strictly higher) and
    /// the pool receives ≤. Net payout in KAS is identical across
    /// tiers — that's the model.
    #[test]
    fn prop_elite_dominates_in_nacho(
        gross in gross_strategy(),
        cfg in fee_config_strategy(),
    ) {
        let s = cfg.compute_allocation(gross, WalletTier::Standard).unwrap();
        let e = cfg.compute_allocation(gross, WalletTier::Elite).unwrap();
        prop_assert_eq!(s.gross_sompi, e.gross_sompi);
        prop_assert_eq!(s.net_payout_sompi, e.net_payout_sompi, "net KAS payout is tier-independent");
        prop_assert!(
            e.nacho_accrual_sompi >= s.nacho_accrual_sompi,
            "elite rebate must dominate standard: elite={} standard={}",
            e.nacho_accrual_sompi, s.nacho_accrual_sompi
        );
        prop_assert!(
            e.pool_fee_sompi <= s.pool_fee_sompi,
            "elite cuts pool revenue, never raises it"
        );
    }

    /// Increasing the topline never increases the miner's net
    /// payout (monotonicity in the operator-facing knob).
    #[test]
    fn prop_higher_topline_means_lower_net(
        gross in gross_strategy(),
        // 0..999 + 1..(1000 - bps_a) keeps bps_b in [bps_a+1, 1000].
        (bps_a, bps_b) in (0_u16..=999).prop_flat_map(|a| (Just(a), (a + 1)..=1_000)),
        tier in tier_strategy(),
    ) {
        let cfg_a = FeeConfig::new(bps_a).unwrap();
        let cfg_b = FeeConfig::new(bps_b).unwrap();
        let a = cfg_a.compute_allocation(gross, tier).unwrap();
        let b = cfg_b.compute_allocation(gross, tier).unwrap();
        prop_assert!(
            b.net_payout_sompi <= a.net_payout_sompi,
            "higher topline must produce equal-or-lower net: bps_a={} a.net={} bps_b={} b.net={}",
            bps_a, a.net_payout_sompi, bps_b, b.net_payout_sompi
        );
    }

    /// Zero gross → zero everywhere.
    #[test]
    fn prop_zero_gross_yields_zero_allocation(
        cfg in fee_config_strategy(),
        tier in tier_strategy(),
    ) {
        let a = cfg.compute_allocation(0, tier).unwrap();
        prop_assert_eq!(a.pool_fee_sompi, 0);
        prop_assert_eq!(a.nacho_accrual_sompi, 0);
        prop_assert_eq!(a.net_payout_sompi, 0);
    }

    /// Zero topline → entire gross flows to the miner as net; pool
    /// and NACHO accrual are zero regardless of tier.
    #[test]
    fn prop_zero_topline_means_full_payout(
        gross in gross_strategy(),
        tier in tier_strategy(),
    ) {
        let cfg = FeeConfig::new(0).unwrap();
        let a = cfg.compute_allocation(gross, tier).unwrap();
        prop_assert_eq!(a.pool_fee_sompi, 0);
        prop_assert_eq!(a.nacho_accrual_sompi, 0);
        prop_assert_eq!(a.net_payout_sompi, gross);
    }

    /// Audit-trail fields always match the inputs used at
    /// computation time. M3's schema migration will persist these
    /// — if they drift from the config, historical allocations
    /// become non-reproducible.
    #[test]
    fn prop_audit_trail_matches_inputs(
        gross in gross_strategy(),
        cfg in fee_config_strategy(),
        tier in tier_strategy(),
    ) {
        let a = cfg.compute_allocation(gross, tier).unwrap();
        prop_assert_eq!(a.applied_topline_bps, cfg.topline_bps());
        prop_assert_eq!(a.applied_rebate_bps, cfg.rebate_bps(tier));
        prop_assert_eq!(a.applied_tier, tier);
    }

    /// ZKas has no NACHO rebate rail, so Elite does not accrue a rebate.
    #[test]
    fn prop_elite_has_no_nacho_rebate(
        gross in gross_strategy(),
        cfg in fee_config_strategy(),
    ) {
        let a = cfg.compute_allocation(gross, WalletTier::Elite).unwrap();
        prop_assert_eq!(a.nacho_accrual_sompi, 0);
    }

    /// ZKas has no NACHO rebate rail, so Standard does not accrue a rebate.
    #[test]
    fn prop_standard_has_no_nacho_rebate(
        gross in gross_strategy(),
        cfg in fee_config_strategy(),
    ) {
        let a = cfg.compute_allocation(gross, WalletTier::Standard).unwrap();
        prop_assert_eq!(a.nacho_accrual_sompi, 0);
    }
}

// ---- Spot tests for boundary cases that proptest may miss --------

#[test]
fn negative_gross_rejected() {
    let cfg = FeeConfig::new(75).unwrap();
    let err = cfg
        .compute_allocation(-1, WalletTier::Standard)
        .unwrap_err();
    assert!(matches!(err, AllocationError::NegativeGross { .. }));
}

#[test]
fn default_topline_known_values() {
    // Spot-checked hand-computed example so future refactors that
    // change the math break loudly with a recognisable failure.
    let cfg = FeeConfig::new(75).unwrap(); // 0.75%
    let alloc = cfg
        .compute_allocation(1_000_000_000, WalletTier::Standard)
        .unwrap();
    // fee_share = 1_000_000_000 * 75 / 10_000 = 7_500_000
    // ZKas has no NACHO rail, so the pool retains the fee share.
    // net       = 1_000_000_000 - 7_500_000 = 992_500_000
    assert_eq!(alloc.pool_fee_sompi, 7_500_000);
    assert_eq!(alloc.nacho_accrual_sompi, 0);
    assert_eq!(alloc.net_payout_sompi, 992_500_000);
    assert!(alloc.is_balanced());

    let elite = cfg
        .compute_allocation(1_000_000_000, WalletTier::Elite)
        .unwrap();
    assert_eq!(elite.pool_fee_sompi, 7_500_000);
    assert_eq!(elite.nacho_accrual_sompi, 0);
    assert_eq!(elite.net_payout_sompi, 992_500_000);
}

#[test]
fn extreme_gross_surfaces_overflow_error_not_panic() {
    let cfg = FeeConfig::new(1_000).unwrap(); // max bps
    // i64::MAX / 10_000 ≈ 9.22e14. Pick something well above it.
    let err = cfg
        .compute_allocation(i64::MAX, WalletTier::Standard)
        .unwrap_err();
    assert!(
        matches!(err, AllocationError::Overflow { stage: "fee_share" }),
        "got: {err:?}"
    );
}

#[test]
fn allocation_is_balanced_returns_false_when_mangled() {
    let mut a = Allocation {
        gross_sompi: 1000,
        pool_fee_sompi: 500,
        nacho_accrual_sompi: 0,
        net_payout_sompi: 500,
        applied_topline_bps: 75,
        applied_rebate_bps: 3300,
        applied_tier: WalletTier::Standard,
    };
    assert!(a.is_balanced());
    a.pool_fee_sompi += 1;
    assert!(!a.is_balanced());
}
