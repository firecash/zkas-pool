//! Pool fee model + tier classification configuration.
//!
//! Loaded once at startup and held read-only thereafter. The
//! topline fee is operator-tunable via env (`KATPOOL_FEE_TOPLINE_BPS`);
//! the two rebate ratios (33% for `Standard`, 100% for `Elite`)
//! are *fixed in code* per `docs/decisions/0012-fee-model-and-tier-classification.md`.
//!
//! Money math is integer basis points end-to-end. Floating-point
//! never touches a sompi figure that the pool keeps or pays out.
//!
//! ## The math, in one place
//!
//! For a wallet's gross share `G` of a block coinbase, with the
//! configured topline fee `T_bps` and the wallet's tier rebate
//! `R_bps`:
//!
//! ```text
//! fee_share     = G * T_bps / 10_000             // taken off the top
//! nacho_accrual = fee_share * R_bps / 10_000     // sompi-equivalent NACHO to miner
//! pool_fee      = fee_share - nacho_accrual      // sompi pool keeps
//! net_payout    = G - fee_share                   // KAS to miner
//! ```
//!
//! The balance equation `G = pool_fee + nacho_accrual + net_payout`
//! holds by construction and is enforced again by the schema's
//! `share_allocation_balance` CHECK.
//!
//! ## NACHO denomination
//!
//! `nacho_accrual` is **sompi**, not NACHO tokens. The KAS→NACHO
//! conversion happens only at krc-20 payout-cycle time, at the
//! prevailing market rate, so per-block accrual stays in the hard
//! asset we're mining.

use std::env;

use serde::{Deserialize, Serialize};

use crate::error::AccountantError;

/// Maximum allowed topline fee, in basis points. 1 000 bps = 10%.
/// Defensive ceiling against operator typos — a real pool will
/// never charge anything close to this.
const MAX_TOPLINE_BPS: u16 = 1_000;

/// Rebate ratio for `Standard`-tier miners.
///
/// **ZKas: 0.** The upstream NACHO/KRC-20 rebate rail (Kasplex indexer +
/// KRC-20 payout engine) does not exist on ZKas, so any non-zero accrual
/// would pile up in `nacho_rebate` forever with no way to ever pay it —
/// a silent fee black hole that also misreports the effective pool fee.
/// The whole topline fee is therefore pool fee. The accrual mechanism is
/// kept (zeroed) so re-enabling a rebate later is a one-constant change.
pub const STANDARD_REBATE_BPS: u16 = 0;

/// Rebate ratio for `Elite`-tier miners. **ZKas: 0** — same rationale as
/// [`STANDARD_REBATE_BPS`]; the tier classifier is inert on ZKas anyway
/// (`StaticTierClassifier::standard` is the wired default).
pub const ELITE_REBATE_BPS: u16 = 0;

/// Default topline fee if `KATPOOL_FEE_TOPLINE_BPS` is unset: 75 bps = 0.75%.
pub const DEFAULT_TOPLINE_BPS: u16 = 75;

/// Pool fee model, derived from env at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeeConfig {
    /// Topline fee in basis points. Default `DEFAULT_TOPLINE_BPS`.
    topline_bps: u16,
}

impl FeeConfig {
    /// Construct directly, validating the basis-point value. Most
    /// callers want [`Self::from_env`].
    pub const fn new(topline_bps: u16) -> Result<Self, &'static str> {
        if topline_bps > MAX_TOPLINE_BPS {
            return Err("topline_bps exceeds MAX_TOPLINE_BPS");
        }
        Ok(Self { topline_bps })
    }

    /// Load from environment. Reads `KATPOOL_FEE_TOPLINE_BPS`;
    /// defaults to `DEFAULT_TOPLINE_BPS` if unset.
    pub fn from_env() -> Result<Self, AccountantError> {
        Self::from_lookup("KATPOOL_FEE_TOPLINE_BPS", |k: &str| env::var(k))
    }

    /// Loader generic over the env-lookup function (testable
    /// without touching process state).
    ///
    /// The closure mirrors `std::env::var`: `Ok(value)` for a set
    /// var, `Err(VarError::NotPresent)` for unset, `Err(NotUnicode)`
    /// for non-UTF-8.
    pub fn from_lookup<F>(var: &str, lookup: F) -> Result<Self, AccountantError>
    where
        F: Fn(&str) -> Result<String, env::VarError>,
    {
        let topline_bps = match lookup(var) {
            Ok(s) => Self::parse_bps(var, &s)?,
            Err(env::VarError::NotPresent) => DEFAULT_TOPLINE_BPS,
            Err(env::VarError::NotUnicode(_)) => {
                return Err(AccountantError::Config(format!("{var} is not valid UTF-8")));
            }
        };
        Self::new(topline_bps)
            .map_err(|e| AccountantError::Config(format!("{var}={topline_bps}: {e}")))
    }

    /// Parse a raw env-string into a basis-point value. Pure;
    /// safe to call from tests without env mutation.
    pub fn parse_bps(var: &str, raw: &str) -> Result<u16, AccountantError> {
        raw.parse::<u16>()
            .map_err(|e| AccountantError::Config(format!("{var}='{raw}' is not a u16: {e}")))
    }

    /// Topline fee in basis points.
    #[must_use]
    pub const fn topline_bps(self) -> u16 {
        self.topline_bps
    }

    /// Rebate ratio for the given tier, in basis points.
    #[must_use]
    pub const fn rebate_bps(self, tier: WalletTier) -> u16 {
        match tier {
            WalletTier::Standard => STANDARD_REBATE_BPS,
            WalletTier::Elite => ELITE_REBATE_BPS,
        }
    }
}

/// Wallet's fee tier for one allocation. Determined at block-
/// maturity time by the [`TierClassifier`](crate::tier::TierClassifier)
/// — never derived from share-time state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "wallet_tier", rename_all = "snake_case")]
pub enum WalletTier {
    /// Default tier; 33% NACHO rebate on the topline fee.
    Standard,
    /// Holds at least one `NACHO` KRC-721 token, OR at least one
    /// `KATCLAIM` KRC-721 token, OR ≥ 100M NACHO (10^16 base units at
    /// 8 decimals). Any single trigger qualifies. 100% NACHO rebate on
    /// the topline fee.
    Elite,
}

impl WalletTier {
    /// Stable lowercase string suitable for metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Elite => "elite",
        }
    }
}

/// One wallet's allocation breakdown for a single block. Computed
/// from a `(gross, FeeConfig, WalletTier)` tuple by
/// [`FeeConfig::compute_allocation`].
///
/// Always satisfies the schema's `share_allocation_balance`
/// invariant: `gross == pool_fee + nacho_accrual + net_payout`.
/// Held in integer sompi end-to-end; no rounding loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Allocation {
    /// Wallet's gross share of the matured coinbase reward.
    pub gross_sompi: i64,
    /// Sompi the pool keeps net of the NACHO rebate.
    pub pool_fee_sompi: i64,
    /// Sompi-equivalent NACHO rebated to the miner (settled at
    /// krc-20 payout-cycle time).
    pub nacho_accrual_sompi: i64,
    /// KAS paid out to the miner.
    pub net_payout_sompi: i64,
    /// Topline-fee basis points that were applied (audit trail).
    pub applied_topline_bps: u16,
    /// Rebate basis points that were applied (audit trail).
    pub applied_rebate_bps: u16,
    /// Tier that was applied (audit trail).
    pub applied_tier: WalletTier,
}

impl Allocation {
    /// Verify the four-way balance equation. The schema's CHECK
    /// constraint enforces this server-side; callers may want a
    /// client-side guard for tight loops.
    #[must_use]
    pub const fn is_balanced(&self) -> bool {
        self.gross_sompi == self.pool_fee_sompi + self.nacho_accrual_sompi + self.net_payout_sompi
    }
}

impl FeeConfig {
    /// Compute one wallet's `Allocation` from its `gross` sompi share
    /// of a block reward, applying the configured topline and the
    /// tier's rebate ratio.
    ///
    /// All math is integer truncation:
    /// ```text
    /// fee_share     = gross * topline_bps / 10_000
    /// nacho_accrual = fee_share * rebate_bps / 10_000
    /// pool_fee      = fee_share - nacho_accrual
    /// net_payout    = gross - fee_share
    /// ```
    /// Truncation residues stay with the pool: `pool_fee` absorbs
    /// every dropped sub-sompi remainder, so the balance equation
    /// holds exactly without rounding tricks.
    ///
    /// Returns `Err` only on `gross < 0`. Both `topline_bps` and
    /// `rebate_bps(tier)` are `u16` and the constants enforce
    /// `topline_bps <= 1_000`, `rebate_bps <= 10_000`, so the
    /// math is total elsewhere.
    // Integer division is denied workspace-wide because most of our
    // money math should be integer-explicit; the allocation routine
    // is the one place where integer truncation IS the semantics
    // (per the function-level doc above), so a locally-scoped allow
    // is the honest expression of intent.
    #[allow(clippy::integer_division)]
    pub fn compute_allocation(
        self,
        gross_sompi: i64,
        tier: WalletTier,
    ) -> Result<Allocation, AllocationError> {
        if gross_sompi < 0 {
            return Err(AllocationError::NegativeGross { gross_sompi });
        }
        let topline = i64::from(self.topline_bps());
        let rebate = i64::from(self.rebate_bps(tier));

        // `gross * topline` is bounded by i64::MAX / 10_000 once
        // gross stays under ~9.2e14 (which covers the entire Kaspa
        // supply cap in sompi). i64::checked_mul guards regardless.
        let fee_share = gross_sompi
            .checked_mul(topline)
            .ok_or(AllocationError::Overflow { stage: "fee_share" })?
            / 10_000;
        let nacho_accrual = fee_share
            .checked_mul(rebate)
            .ok_or(AllocationError::Overflow {
                stage: "nacho_accrual",
            })?
            / 10_000;
        // pool_fee absorbs the rebate-side truncation residue.
        let pool_fee = fee_share - nacho_accrual;
        let net_payout = gross_sompi - fee_share;

        let allocation = Allocation {
            gross_sompi,
            pool_fee_sompi: pool_fee,
            nacho_accrual_sompi: nacho_accrual,
            net_payout_sompi: net_payout,
            applied_topline_bps: self.topline_bps(),
            applied_rebate_bps: self.rebate_bps(tier),
            applied_tier: tier,
        };
        // Belt-and-braces — fires only on a code bug (the math
        // above is total within the validated input ranges, so
        // this branch is unreachable in practice).
        if !allocation.is_balanced() {
            return Err(AllocationError::Unbalanced(allocation));
        }
        Ok(allocation)
    }
}

/// Errors from [`FeeConfig::compute_allocation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AllocationError {
    /// Gross sompi value was negative. Allocations never make
    /// sense for negative grosses; surfaces a caller bug.
    #[error("gross_sompi must be >= 0, got {gross_sompi}")]
    NegativeGross {
        /// The offending value.
        gross_sompi: i64,
    },
    /// An intermediate multiplication overflowed `i64`. Unreachable
    /// in practice within the Kaspa supply cap; kept as an explicit
    /// error so a future supply-cap change doesn't silently wrap.
    #[error("integer overflow at stage `{stage}`")]
    Overflow {
        /// Which arithmetic step blew the limit.
        stage: &'static str,
    },
    /// Belt-and-braces guard — the balance equation didn't hold
    /// after computation. Unreachable within the validated input
    /// ranges; reaching this variant means a code bug.
    #[error("allocation failed balance check: {0:?}")]
    Unbalanced(Allocation),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
    use super::*;

    #[test]
    fn default_topline_is_75_bps() {
        assert_eq!(DEFAULT_TOPLINE_BPS, 75);
    }

    #[test]
    fn rebate_ratios_match_spec() {
        // ZKas: the NACHO/KRC-20 rebate rail doesn't exist, so both tiers
        // accrue nothing — the whole topline fee is pool fee.
        assert_eq!(STANDARD_REBATE_BPS, 0, "no rebate rail on ZKas");
        assert_eq!(ELITE_REBATE_BPS, 0, "no rebate rail on ZKas");
    }

    fn lookup_unset(_: &str) -> Result<String, env::VarError> {
        Err(env::VarError::NotPresent)
    }
    fn lookup_returning(value: &'static str) -> impl Fn(&str) -> Result<String, env::VarError> {
        move |_: &str| Ok(value.to_owned())
    }

    #[test]
    fn from_lookup_defaults_when_unset() {
        let cfg = FeeConfig::from_lookup("KATPOOL_FEE_TOPLINE_BPS", lookup_unset).unwrap();
        assert_eq!(cfg.topline_bps(), DEFAULT_TOPLINE_BPS);
    }

    #[test]
    fn from_lookup_accepts_valid_value() {
        let cfg =
            FeeConfig::from_lookup("KATPOOL_FEE_TOPLINE_BPS", lookup_returning("50")).unwrap();
        assert_eq!(cfg.topline_bps(), 50);
    }

    #[test]
    fn from_lookup_accepts_zero_topline() {
        // A pool may legitimately run at zero fee (community pool,
        // promotional period). The schema validates >=0 not >0;
        // we mirror that here.
        let cfg = FeeConfig::from_lookup("KATPOOL_FEE_TOPLINE_BPS", lookup_returning("0")).unwrap();
        assert_eq!(cfg.topline_bps(), 0);
    }

    #[test]
    fn from_lookup_rejects_too_large() {
        let err = FeeConfig::from_lookup("KATPOOL_FEE_TOPLINE_BPS", lookup_returning("5000"))
            .unwrap_err();
        assert!(format!("{err}").contains("MAX_TOPLINE_BPS"));
    }

    #[test]
    fn from_lookup_rejects_non_numeric() {
        assert!(
            FeeConfig::from_lookup("KATPOOL_FEE_TOPLINE_BPS", lookup_returning("abc")).is_err()
        );
    }

    #[test]
    fn from_lookup_rejects_negative_string() {
        assert!(FeeConfig::from_lookup("KATPOOL_FEE_TOPLINE_BPS", lookup_returning("-1")).is_err());
    }

    #[test]
    fn rebate_bps_by_tier() {
        let cfg = FeeConfig::new(75).unwrap();
        assert_eq!(cfg.rebate_bps(WalletTier::Standard), STANDARD_REBATE_BPS);
        assert_eq!(cfg.rebate_bps(WalletTier::Elite), ELITE_REBATE_BPS);
    }

    #[test]
    fn tier_as_str_is_stable() {
        assert_eq!(WalletTier::Standard.as_str(), "standard");
        assert_eq!(WalletTier::Elite.as_str(), "elite");
    }
}
