//! Payout **policy**: how a miner's vested balance actually leaves the pool.
//!
//! FireCash's payout has exactly two paths, and this module is the single place
//! that decides which rewards each one moves and whether a signature is required:
//!
//! - **[`PayoutTrigger::AutoSweep`]** — the pool periodically sweeps every reward
//!   that has reached the [`VESTING_CLIFF`](crate::vesting::VESTING_CLIFF) and sends
//!   it to the miner's shielded address at **100%**. This needs **no signature and
//!   no action from the miner**: once a reward is ≥ 10 days old it is auto-sent.
//!   Rewards still inside the cliff are *deferred* — left untouched to keep vesting.
//!
//! - **[`PayoutTrigger::SignedClaim`]** — a miner who wants their money *before* the
//!   cliff initiates a claim. This pays **everything**: matured rewards at 100% and
//!   still-vesting rewards at **50%** (the early-payout haircut). Because it moves
//!   funds early and on demand, it is authenticated by a shielded signature
//!   ([`crate::vesting`] does the split; `api::claim` does the signature challenge).
//!
//! The rule the user set, stated once: *signing is only needed to claim before the
//! 10-day cliff; after the cliff, rewards auto-send at full value.* A signed claim
//! is therefore purely a miner's choice to take a 50% haircut for speed — never a
//! requirement to eventually get paid.
//!
//! All splitting delegates to [`vest_reward`]/[`vest_claim`], so the value-conserving
//! `miner + forfeit == gross` invariant holds here too.

use std::time::Duration;

use crate::vesting::{vest_claim, vest_reward, ClaimTotals, VestedSplit, VESTING_CLIFF};

/// What is moving a payout: an automatic maturity sweep, or a miner-initiated
/// early claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayoutTrigger {
    /// Scheduled sweep of matured rewards. No signature; matured (≥ cliff) rewards
    /// only, each at 100%. Unmatured rewards are deferred to a later sweep.
    AutoSweep,
    /// Miner-initiated claim before the cliff. Requires a signature. Pays all
    /// rewards: matured at 100%, still-vesting at 50%.
    SignedClaim,
}

impl PayoutTrigger {
    /// Whether this trigger requires a shielded signature to authorize.
    ///
    /// Only [`SignedClaim`](Self::SignedClaim) does — an [`AutoSweep`](Self::AutoSweep)
    /// moves only fully-matured funds to the address the pool already recorded, so
    /// there is nothing for a signature to gate.
    #[must_use]
    pub const fn requires_signature(self) -> bool {
        matches!(self, Self::SignedClaim)
    }
}

/// The decision for one payout run against a miner's set of rewards.
///
/// Invariant: `totals.is_balanced()`, and `included_count + deferred_count` equals
/// the number of rewards passed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayoutPlan {
    /// Which path produced this plan.
    pub trigger: PayoutTrigger,
    /// The vested split of every reward this run *includes* (see `included_count`).
    pub totals: ClaimTotals,
    /// `true` iff the miner must present a valid signature before this pays out.
    pub requires_signature: bool,
    /// Rewards this run pays out now.
    pub included_count: u32,
    /// Rewards this run leaves untouched (an auto-sweep defers unmatured rewards;
    /// a signed claim defers nothing).
    pub deferred_count: u32,
}

impl PayoutPlan {
    /// Whether this plan actually moves any funds.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.included_count == 0
    }
}

/// Build the payout plan for `rewards` under `trigger`.
///
/// Each reward is `(gross_sompi, age)` where `age` is the elapsed time since the
/// reward matured into the miner's pool balance.
///
/// - Under [`PayoutTrigger::AutoSweep`], only rewards with `age >= VESTING_CLIFF`
///   are included (each at 100%); the rest are deferred.
/// - Under [`PayoutTrigger::SignedClaim`], every reward is included, split by its
///   own age via [`vest_claim`] (matured 100%, unmatured 50%).
#[must_use]
pub fn plan_payout<I>(rewards: I, trigger: PayoutTrigger) -> PayoutPlan
where
    I: IntoIterator<Item = (i64, Duration)>,
{
    match trigger {
        PayoutTrigger::AutoSweep => {
            let mut totals = ClaimTotals::default();
            let mut included = 0u32;
            let mut deferred = 0u32;
            for (gross, age) in rewards {
                if age >= VESTING_CLIFF {
                    // A matured reward always pays 100%, so its split is exact.
                    let split: VestedSplit = vest_reward(gross, age);
                    debug_assert!(split.matured && split.forfeit_sompi == 0);
                    totals.gross_sompi += split.gross_sompi;
                    totals.miner_sompi += split.miner_sompi;
                    totals.forfeit_sompi += split.forfeit_sompi;
                    totals.matured_count += 1;
                    included += 1;
                } else {
                    deferred += 1;
                }
            }
            PayoutPlan {
                trigger,
                totals,
                requires_signature: false,
                included_count: included,
                deferred_count: deferred,
            }
        }
        PayoutTrigger::SignedClaim => {
            let rewards: Vec<(i64, Duration)> = rewards.into_iter().collect();
            let included = rewards.len() as u32;
            let totals = vest_claim(rewards);
            PayoutPlan {
                trigger,
                totals,
                requires_signature: true,
                included_count: included,
                deferred_count: 0,
            }
        }
    }
}

/// Time until a reward of the given `age` becomes auto-sweepable (reaches the
/// cliff). Returns [`Duration::ZERO`] if it is already matured — i.e. it will be
/// paid, unsigned, on the next sweep.
#[must_use]
pub fn time_until_auto_payout(age: Duration) -> Duration {
    VESTING_CLIFF.saturating_sub(age)
}

/// Whether any reward in `ages` is already matured and would be picked up by the
/// next [`PayoutTrigger::AutoSweep`] with no miner action.
#[must_use]
pub fn has_auto_payable<I>(ages: I) -> bool
where
    I: IntoIterator<Item = Duration>,
{
    ages.into_iter().any(|age| age >= VESTING_CLIFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FC: i64 = 100_000_000;
    const BLOCK_REWARD: i64 = 60 * FC;

    fn days(d: u64) -> Duration {
        Duration::from_secs(d * 24 * 60 * 60)
    }

    #[test]
    fn auto_sweep_needs_no_signature() {
        assert!(!PayoutTrigger::AutoSweep.requires_signature());
        assert!(PayoutTrigger::SignedClaim.requires_signature());
    }

    #[test]
    fn auto_sweep_pays_only_matured_at_full() {
        // one fresh (deferred), two matured (paid 100%).
        let rewards = [
            (BLOCK_REWARD, Duration::ZERO),
            (BLOCK_REWARD, days(10)),
            (BLOCK_REWARD, days(25)),
        ];
        let plan = plan_payout(rewards, PayoutTrigger::AutoSweep);
        assert!(!plan.requires_signature);
        assert_eq!(plan.included_count, 2);
        assert_eq!(plan.deferred_count, 1);
        // Matured rewards pay full, nothing forfeited.
        assert_eq!(plan.totals.miner_sompi, 2 * BLOCK_REWARD);
        assert_eq!(plan.totals.forfeit_sompi, 0);
        assert_eq!(plan.totals.matured_count, 2);
        assert!(plan.totals.is_balanced());
    }

    #[test]
    fn auto_sweep_with_nothing_matured_is_empty() {
        let rewards = [(BLOCK_REWARD, Duration::ZERO), (BLOCK_REWARD, days(9))];
        let plan = plan_payout(rewards, PayoutTrigger::AutoSweep);
        assert!(plan.is_empty());
        assert_eq!(plan.deferred_count, 2);
        assert_eq!(plan.totals.miner_sompi, 0);
        assert_eq!(plan.totals.forfeit_sompi, 0);
    }

    #[test]
    fn signed_claim_pays_everything_with_haircut() {
        // one fresh (50%), one matured (100%). Signed claim takes both.
        let rewards = [(BLOCK_REWARD, Duration::ZERO), (BLOCK_REWARD, days(30))];
        let plan = plan_payout(rewards, PayoutTrigger::SignedClaim);
        assert!(plan.requires_signature);
        assert_eq!(plan.included_count, 2);
        assert_eq!(plan.deferred_count, 0);
        // 30 FC (half of fresh) + 60 FC (full matured) = 90 FC.
        assert_eq!(plan.totals.miner_sompi, 30 * FC + BLOCK_REWARD);
        assert_eq!(plan.totals.forfeit_sompi, 30 * FC);
        assert_eq!(plan.totals.early_count, 1);
        assert_eq!(plan.totals.matured_count, 1);
        assert!(plan.totals.is_balanced());
    }

    #[test]
    fn matured_reward_pays_same_either_path() {
        // A fully-matured reward pays 100% whether auto-swept or signed-claimed:
        // there is never a reason to sign for money you can get free at full value.
        let auto = plan_payout([(BLOCK_REWARD, days(20))], PayoutTrigger::AutoSweep);
        let signed = plan_payout([(BLOCK_REWARD, days(20))], PayoutTrigger::SignedClaim);
        assert_eq!(auto.totals.miner_sompi, BLOCK_REWARD);
        assert_eq!(signed.totals.miner_sompi, BLOCK_REWARD);
    }

    #[test]
    fn time_until_auto_payout_counts_down() {
        assert_eq!(time_until_auto_payout(Duration::ZERO), VESTING_CLIFF);
        assert_eq!(time_until_auto_payout(days(4)), days(6));
        assert_eq!(time_until_auto_payout(days(10)), Duration::ZERO);
        assert_eq!(time_until_auto_payout(days(99)), Duration::ZERO);
    }

    #[test]
    fn has_auto_payable_detects_matured() {
        assert!(!has_auto_payable([Duration::ZERO, days(9)]));
        assert!(has_auto_payable([Duration::ZERO, days(11)]));
    }

    #[test]
    fn empty_reward_set_is_empty_plan() {
        let plan = plan_payout(std::iter::empty(), PayoutTrigger::AutoSweep);
        assert!(plan.is_empty());
        assert_eq!(plan.included_count, 0);
        assert_eq!(plan.deferred_count, 0);
        assert!(plan.totals.is_balanced());
    }
}
