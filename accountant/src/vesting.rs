//! ZKas shielded-payout **cliff vesting**.
//!
//! Every matured coinbase reward credited to a miner vests on a **hard cliff**:
//! a claim settled **before** [`VESTING_CLIFF`] after the reward matured pays
//! [`EARLY_PAYOUT_BPS`] (50%); a claim **at or after** the cliff pays 100%. There
//! is no linear ramp between — the payout steps from 50% to 100% exactly at the
//! cliff. The withheld remainder on an early claim is routed per [`ForfeitPolicy`]
//! (default: the pool operator treasury).
//!
//! All arithmetic is integer sompi with an i128 intermediate; `miner + forfeit`
//! always equals `gross` with no rounding loss, so the split can never create or
//! destroy value.

use std::time::Duration;

/// Days a reward must age before it pays out in full.
pub const VESTING_CLIFF_DAYS: u64 = 10;

/// The hard vesting cliff: rewards younger than this pay [`EARLY_PAYOUT_BPS`].
pub const VESTING_CLIFF: Duration = Duration::from_secs(VESTING_CLIFF_DAYS * 24 * 60 * 60);

/// Payout fraction (basis points) for a reward claimed before the cliff: 50.00%.
pub const EARLY_PAYOUT_BPS: u32 = 5_000;

/// Payout fraction (basis points) for a reward claimed at/after the cliff: 100%.
pub const FULL_PAYOUT_BPS: u32 = 10_000;

/// Where the withheld portion of an early claim goes.
///
/// The pool captures the forfeited half on an early claim; this selects its
/// destination. Only [`Treasury`](Self::Treasury) is wired in the payout engine
/// today — the others are recorded for the accounting audit trail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ForfeitPolicy {
    /// Forfeited sompi accrues to the pool operator treasury. Default.
    #[default]
    Treasury,
    /// Forfeited sompi is redistributed pro-rata to miners still vesting.
    Redistribute,
    /// Forfeited sompi is burned (never re-minted), reducing circulating supply.
    Burn,
}

/// The vested split of a **single** reward at claim time.
///
/// Invariant: `miner_sompi + forfeit_sompi == gross_sompi`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VestedSplit {
    /// The reward's full credited value.
    pub gross_sompi: i64,
    /// Paid to the miner now.
    pub miner_sompi: i64,
    /// Withheld from the miner (routed per [`ForfeitPolicy`]).
    pub forfeit_sompi: i64,
    /// Basis points applied — [`EARLY_PAYOUT_BPS`] or [`FULL_PAYOUT_BPS`] (audit).
    pub applied_bps: u32,
    /// `true` if the reward had reached the cliff (paid in full).
    pub matured: bool,
}

impl VestedSplit {
    /// Verify the two-way balance equation.
    #[must_use]
    pub const fn is_balanced(&self) -> bool {
        self.miner_sompi + self.forfeit_sompi == self.gross_sompi
    }
}

/// Compute the vested split for one reward.
///
/// `age` is the wall-clock elapsed since the reward matured into the miner's
/// balance. The cliff is a `>=` boundary: exactly [`VESTING_CLIFF`] pays in full.
///
/// # Panics
/// Panics if `gross_sompi` is negative (a credited reward is never negative).
#[must_use]
pub fn vest_reward(gross_sompi: i64, age: Duration) -> VestedSplit {
    assert!(gross_sompi >= 0, "gross reward must be non-negative");
    let matured = age >= VESTING_CLIFF;
    let applied_bps = if matured { FULL_PAYOUT_BPS } else { EARLY_PAYOUT_BPS };
    // floor(gross * bps / 10_000); i128 avoids overflow for any i64 gross.
    let miner_sompi =
        (i128::from(gross_sompi) * i128::from(applied_bps) / i128::from(FULL_PAYOUT_BPS)) as i64;
    let forfeit_sompi = gross_sompi - miner_sompi;
    VestedSplit { gross_sompi, miner_sompi, forfeit_sompi, applied_bps, matured }
}

/// Totals over a set of rewards claimed together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClaimTotals {
    /// Sum of every reward's full value.
    pub gross_sompi: i64,
    /// Sum paid to the miner.
    pub miner_sompi: i64,
    /// Sum withheld (routed per [`ForfeitPolicy`]).
    pub forfeit_sompi: i64,
    /// Count of rewards that had matured past the cliff.
    pub matured_count: u32,
    /// Count of rewards still within the cliff (paid at 50%).
    pub early_count: u32,
}

impl ClaimTotals {
    /// Verify the two-way balance equation across the whole claim.
    #[must_use]
    pub const fn is_balanced(&self) -> bool {
        self.miner_sompi + self.forfeit_sompi == self.gross_sompi
    }
}

/// Vest a batch of `(gross_sompi, age)` rewards a miner is claiming at once.
///
/// Each reward vests on its **own** age against the cliff, so a single claim can
/// mix fully-vested and half-vested rewards. Totals sum losslessly.
#[must_use]
pub fn vest_claim<I>(rewards: I) -> ClaimTotals
where
    I: IntoIterator<Item = (i64, Duration)>,
{
    let mut totals = ClaimTotals::default();
    for (gross, age) in rewards {
        let split = vest_reward(gross, age);
        totals.gross_sompi += split.gross_sompi;
        totals.miner_sompi += split.miner_sompi;
        totals.forfeit_sompi += split.forfeit_sompi;
        if split.matured {
            totals.matured_count += 1;
        } else {
            totals.early_count += 1;
        }
    }
    totals
}

#[cfg(test)]
mod tests {
    use super::*;

    const FC: i64 = 100_000_000; // 1 $zkas in sompi
    const BLOCK_REWARD: i64 = 60 * FC; // initial 1-BPS reward

    fn days(d: u64) -> Duration {
        Duration::from_secs(d * 24 * 60 * 60)
    }

    #[test]
    fn immediate_claim_pays_half() {
        let s = vest_reward(BLOCK_REWARD, Duration::ZERO);
        assert_eq!(s.miner_sompi, 30 * FC);
        assert_eq!(s.forfeit_sompi, 30 * FC);
        assert_eq!(s.applied_bps, EARLY_PAYOUT_BPS);
        assert!(!s.matured);
        assert!(s.is_balanced());
    }

    #[test]
    fn just_under_cliff_still_half() {
        let s = vest_reward(BLOCK_REWARD, days(10) - Duration::from_secs(1));
        assert_eq!(s.miner_sompi, 30 * FC);
        assert!(!s.matured);
        assert!(s.is_balanced());
    }

    #[test]
    fn exactly_at_cliff_pays_full() {
        let s = vest_reward(BLOCK_REWARD, VESTING_CLIFF);
        assert_eq!(s.miner_sompi, BLOCK_REWARD);
        assert_eq!(s.forfeit_sompi, 0);
        assert_eq!(s.applied_bps, FULL_PAYOUT_BPS);
        assert!(s.matured);
        assert!(s.is_balanced());
    }

    #[test]
    fn past_cliff_pays_full() {
        let s = vest_reward(BLOCK_REWARD, days(30));
        assert_eq!(s.miner_sompi, BLOCK_REWARD);
        assert_eq!(s.forfeit_sompi, 0);
        assert!(s.matured);
    }

    #[test]
    fn odd_sompi_loses_nothing() {
        // 1 sompi at 50% floors the miner to 0; the whole unit is forfeited.
        let s = vest_reward(1, Duration::ZERO);
        assert_eq!(s.miner_sompi, 0);
        assert_eq!(s.forfeit_sompi, 1);
        assert!(s.is_balanced());
        // 3 sompi at 50% -> miner 1, forfeit 2, still balanced.
        let s = vest_reward(3, Duration::ZERO);
        assert_eq!(s.miner_sompi, 1);
        assert_eq!(s.forfeit_sompi, 2);
        assert!(s.is_balanced());
    }

    #[test]
    fn zero_reward_is_zero() {
        let s = vest_reward(0, Duration::ZERO);
        assert_eq!(s.miner_sompi, 0);
        assert_eq!(s.forfeit_sompi, 0);
        assert!(s.is_balanced());
    }

    #[test]
    fn batch_mixes_early_and_matured() {
        // three rewards: one fresh (50%), two past the cliff (100%).
        let rewards = [
            (BLOCK_REWARD, Duration::ZERO),
            (BLOCK_REWARD, days(11)),
            (BLOCK_REWARD, days(40)),
        ];
        let t = vest_claim(rewards);
        assert_eq!(t.gross_sompi, 3 * BLOCK_REWARD);
        assert_eq!(t.miner_sompi, 30 * FC + BLOCK_REWARD + BLOCK_REWARD);
        assert_eq!(t.forfeit_sompi, 30 * FC);
        assert_eq!(t.early_count, 1);
        assert_eq!(t.matured_count, 2);
        assert!(t.is_balanced());
    }

    #[test]
    fn max_reward_does_not_overflow() {
        let s = vest_reward(i64::MAX, Duration::ZERO);
        assert!(s.is_balanced());
        assert_eq!(s.miner_sompi, i64::MAX / 2);
    }
}
