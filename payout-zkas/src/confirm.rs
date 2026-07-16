//! Pure confirmation policy for shielded payouts (no I/O).
//!
//! A shielded transaction leaves no transparent change coin to watch, so
//! (unlike `payout-kas`) acceptance is observed from the **virtual chain's
//! accepted-transaction-id stream** (`get_virtual_chain_from_block`), which
//! includes shielded transactions like any other. The states mirror the
//! upstream money-safety posture: an unobservable transaction is `Unknown`,
//! never auto-failed, so funds can never be re-sent by a false negative.

/// DAA-score depth after which an accepted ZKas payout is treated as
/// settled. 100 DAA ≈ 100 seconds at ZKas's 1 BPS.
pub const ZKAS_PAYOUT_CONFIRMATION_DAA: u64 = 100;

/// Derived state of a submitted shielded payout transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmationState {
    /// Still in the mempool, awaiting block inclusion.
    Pending,
    /// Seen in a chain block's accepted-transaction ids, below depth.
    Accepted,
    /// Accepted and matured past [`ZKAS_PAYOUT_CONFIRMATION_DAA`].
    Confirmed,
    /// Neither in the mempool nor observed accepted. No state change —
    /// resolved by a later pass or operator reconciliation, never
    /// auto-failed.
    Unknown,
}

/// Reads gathered for one submitted transaction.
#[derive(Debug, Clone, Copy)]
pub struct ConfirmationInputs {
    /// Current virtual DAA score.
    pub virtual_daa_score: u64,
    /// Whether the txid is still in the mempool.
    pub in_mempool: bool,
    /// Accepting chain block's DAA score, observed this pass or durably
    /// recorded by a previous pass (`payout.accepted_daa_score`).
    pub accept_daa: Option<u64>,
}

/// Fold raw chain reads into a [`ConfirmationState`]. Acceptance wins over
/// a stale mempool read (a tx can be both briefly).
#[must_use]
pub const fn classify_confirmation(inputs: ConfirmationInputs, confirmation_daa: u64) -> ConfirmationState {
    if let Some(accept) = inputs.accept_daa {
        let depth = inputs.virtual_daa_score.saturating_sub(accept);
        if depth >= confirmation_daa {
            return ConfirmationState::Confirmed;
        }
        return ConfirmationState::Accepted;
    }
    if inputs.in_mempool {
        return ConfirmationState::Pending;
    }
    ConfirmationState::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONF: u64 = ZKAS_PAYOUT_CONFIRMATION_DAA;

    #[test]
    fn mempool_is_pending() {
        let s = classify_confirmation(
            ConfirmationInputs { virtual_daa_score: 1_000, in_mempool: true, accept_daa: None },
            CONF,
        );
        assert_eq!(s, ConfirmationState::Pending);
    }

    #[test]
    fn accepted_below_depth() {
        let s = classify_confirmation(
            ConfirmationInputs { virtual_daa_score: 1_050, in_mempool: false, accept_daa: Some(1_000) },
            CONF,
        );
        assert_eq!(s, ConfirmationState::Accepted);
    }

    #[test]
    fn confirmed_at_depth() {
        let s = classify_confirmation(
            ConfirmationInputs { virtual_daa_score: 1_100, in_mempool: false, accept_daa: Some(1_000) },
            CONF,
        );
        assert_eq!(s, ConfirmationState::Confirmed);
    }

    #[test]
    fn acceptance_wins_over_stale_mempool_read() {
        let s = classify_confirmation(
            ConfirmationInputs { virtual_daa_score: 1_100, in_mempool: true, accept_daa: Some(1_000) },
            CONF,
        );
        assert_eq!(s, ConfirmationState::Confirmed);
    }

    #[test]
    fn unobservable_is_unknown_never_failed() {
        let s = classify_confirmation(
            ConfirmationInputs { virtual_daa_score: 1_000, in_mempool: false, accept_daa: None },
            CONF,
        );
        assert_eq!(s, ConfirmationState::Unknown);
    }
}
