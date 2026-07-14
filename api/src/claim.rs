//! Signature-authenticated payout **claims**.
//!
//! A miner's pool balance is custodial until claimed. Because ZKas addresses
//! are shielded, a claim is authenticated by a **shielded signature**, not a
//! password: the miner proves control of their payout address by signing a
//! server-issued, single-use **challenge** with their Orchard spend key. There is
//! no mining-password requirement — the signature is the security boundary.
//!
//! Flow:
//! 1. `POST /claim/challenge {address}` → [`ChallengeStore::issue`] returns a random
//!    nonce bound to that address, valid for [`ChallengeStore::ttl`].
//! 2. Miner signs the nonce with their shielded key (e.g. `shielded-pay sign`),
//!    producing `(fvk, sig)`.
//! 3. `POST /claim {address, fvk, sig}` → [`ChallengeStore::redeem`] consumes the
//!    challenge (single-use) and the injected [`SignatureVerifier`] checks the
//!    signature; on success the payout engine pays the vested balance.
//!
//! The signature check itself is delegated to a [`SignatureVerifier`] — in
//! production, `shielded_core::message::verify_message`, which binds the presented
//! FVK to the claimed address and verifies the RedPallas signature. Injecting it
//! keeps this module (and its tests) free of the Orchard/Halo2 dependency.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Length of an issued challenge nonce, in bytes.
pub const CHALLENGE_LEN: usize = 32;

/// Default challenge lifetime.
pub const DEFAULT_CHALLENGE_TTL: Duration = Duration::from_secs(300);

/// A challenge nonce a miner must sign to claim.
pub type Challenge = [u8; CHALLENGE_LEN];

/// Outcome of verifying a signed claim.
#[derive(Debug, PartialEq, Eq)]
pub enum ClaimError {
    /// No live challenge exists for this address (never issued, already used, or expired).
    NoChallenge,
    /// The presented signature did not verify for the challenge + address.
    BadSignature,
}

/// Verifies that `sig`/`fvk` prove control of `address` over the signed `challenge`.
///
/// Production impl wraps `shielded_core::message::verify_message`. Abstracted so
/// this crate need not depend on Orchard, and so the challenge lifecycle is
/// testable with a deterministic stub.
pub trait SignatureVerifier {
    /// Return `true` iff the signature is valid for `(address, challenge, fvk, sig)`.
    fn verify(&self, address: &[u8], challenge: &Challenge, fvk: &[u8], sig: &[u8]) -> bool;
}

/// A live, unexpired challenge for one address.
struct Pending {
    challenge: Challenge,
    expires_at: Instant,
}

/// Issues and redeems single-use, address-bound, expiring challenges.
///
/// One challenge per address at a time: re-issuing overwrites the prior one, so a
/// stale challenge can never be redeemed after a fresh request. Redemption is
/// **single-use** — a successful *or* signature-failed redeem consumes the
/// challenge, so a captured signature cannot be replayed.
pub struct ChallengeStore {
    ttl: Duration,
    pending: Mutex<HashMap<Vec<u8>, Pending>>,
}

impl ChallengeStore {
    /// Create a store with the given challenge lifetime.
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self { ttl, pending: Mutex::new(HashMap::new()) }
    }

    /// Challenge lifetime.
    #[must_use]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Issue a fresh challenge for `address`, overwriting any prior live one.
    /// `now` and `nonce` are injected so the caller controls the clock and RNG
    /// (production passes `Instant::now()` and a CSPRNG-filled array).
    pub fn issue_at(&self, address: &[u8], nonce: Challenge, now: Instant) -> Challenge {
        let mut map = self.pending.lock().expect("challenge mutex poisoned");
        map.insert(address.to_vec(), Pending { challenge: nonce, expires_at: now + self.ttl });
        nonce
    }

    /// Redeem a signed claim for `address`. Consumes the address's challenge
    /// **unconditionally** (single-use), then verifies. Returns the redeemed
    /// challenge on success so the caller can bind an audit record to it.
    pub fn redeem_at<V: SignatureVerifier>(
        &self,
        address: &[u8],
        fvk: &[u8],
        sig: &[u8],
        verifier: &V,
        now: Instant,
    ) -> Result<Challenge, ClaimError> {
        // Take the challenge out regardless of outcome: single-use, no replay.
        let pending = {
            let mut map = self.pending.lock().expect("challenge mutex poisoned");
            map.remove(address)
        };
        let pending = pending.ok_or(ClaimError::NoChallenge)?;
        if now >= pending.expires_at {
            return Err(ClaimError::NoChallenge);
        }
        if verifier.verify(address, &pending.challenge, fvk, sig) {
            Ok(pending.challenge)
        } else {
            Err(ClaimError::BadSignature)
        }
    }

    /// Drop expired challenges. Call periodically to bound memory.
    pub fn sweep_expired(&self, now: Instant) {
        let mut map = self.pending.lock().expect("challenge mutex poisoned");
        map.retain(|_, p| now < p.expires_at);
    }

    /// Number of live (possibly-expired-but-unswept) challenges. Test/telemetry aid.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pending.lock().expect("challenge mutex poisoned").len()
    }

    /// Whether the store holds no challenges.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for ChallengeStore {
    fn default() -> Self {
        Self::new(DEFAULT_CHALLENGE_TTL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Accepts only the exact (address, challenge, fvk, sig) it was told to.
    struct StubVerifier {
        ok_challenge: Challenge,
        ok_sig: Vec<u8>,
    }
    impl SignatureVerifier for StubVerifier {
        fn verify(&self, _address: &[u8], challenge: &Challenge, _fvk: &[u8], sig: &[u8]) -> bool {
            *challenge == self.ok_challenge && sig == self.ok_sig.as_slice()
        }
    }

    const ADDR: &[u8] = b"firecash:pyaddr";
    const FVK: &[u8] = &[7u8; 96];

    fn nonce(b: u8) -> Challenge {
        [b; CHALLENGE_LEN]
    }

    #[test]
    fn issue_then_valid_redeem_succeeds() {
        let store = ChallengeStore::new(Duration::from_secs(300));
        let t0 = Instant::now();
        let ch = store.issue_at(ADDR, nonce(1), t0);
        let v = StubVerifier { ok_challenge: ch, ok_sig: b"goodsig".to_vec() };
        let out = store.redeem_at(ADDR, FVK, b"goodsig", &v, t0 + Duration::from_secs(10));
        assert_eq!(out, Ok(ch));
        assert!(store.is_empty(), "challenge consumed on success");
    }

    #[test]
    fn redeem_without_challenge_fails() {
        let store = ChallengeStore::default();
        let v = StubVerifier { ok_challenge: nonce(1), ok_sig: b"x".to_vec() };
        assert_eq!(
            store.redeem_at(ADDR, FVK, b"x", &v, Instant::now()),
            Err(ClaimError::NoChallenge)
        );
    }

    #[test]
    fn bad_signature_still_consumes_challenge() {
        let store = ChallengeStore::default();
        let t0 = Instant::now();
        let ch = store.issue_at(ADDR, nonce(2), t0);
        let v = StubVerifier { ok_challenge: ch, ok_sig: b"goodsig".to_vec() };
        // Wrong signature: rejected AND consumed (no replay of a captured attempt).
        assert_eq!(store.redeem_at(ADDR, FVK, b"WRONG", &v, t0), Err(ClaimError::BadSignature));
        assert!(store.is_empty());
        // A replay of even the correct signature now finds no challenge.
        assert_eq!(
            store.redeem_at(ADDR, FVK, b"goodsig", &v, t0),
            Err(ClaimError::NoChallenge)
        );
    }

    #[test]
    fn expired_challenge_is_rejected() {
        let store = ChallengeStore::new(Duration::from_secs(60));
        let t0 = Instant::now();
        let ch = store.issue_at(ADDR, nonce(3), t0);
        let v = StubVerifier { ok_challenge: ch, ok_sig: b"goodsig".to_vec() };
        // At exactly ttl the challenge is expired (>= boundary).
        assert_eq!(
            store.redeem_at(ADDR, FVK, b"goodsig", &v, t0 + Duration::from_secs(60)),
            Err(ClaimError::NoChallenge)
        );
    }

    #[test]
    fn reissue_overwrites_stale_challenge() {
        let store = ChallengeStore::default();
        let t0 = Instant::now();
        store.issue_at(ADDR, nonce(4), t0);
        let fresh = store.issue_at(ADDR, nonce(5), t0);
        let v = StubVerifier { ok_challenge: nonce(4), ok_sig: b"s".to_vec() };
        // The stale nonce(4) can't be redeemed; only the fresh one is live.
        assert_eq!(store.redeem_at(ADDR, FVK, b"s", &v, t0), Err(ClaimError::BadSignature));
        assert_eq!(store.len(), 0);
        // Prove the fresh one was the live challenge.
        let store2 = ChallengeStore::default();
        store2.issue_at(ADDR, nonce(4), t0);
        store2.issue_at(ADDR, fresh, t0);
        let v2 = StubVerifier { ok_challenge: fresh, ok_sig: b"s".to_vec() };
        assert_eq!(store2.redeem_at(ADDR, FVK, b"s", &v2, t0), Ok(fresh));
    }

    #[test]
    fn sweep_drops_only_expired() {
        let store = ChallengeStore::new(Duration::from_secs(60));
        let t0 = Instant::now();
        store.issue_at(b"a", nonce(1), t0);
        store.issue_at(b"b", nonce(2), t0 + Duration::from_secs(120));
        store.sweep_expired(t0 + Duration::from_secs(90));
        assert_eq!(store.len(), 1, "only the still-live challenge remains");
    }

    #[test]
    fn per_address_isolation() {
        let store = ChallengeStore::default();
        let t0 = Instant::now();
        let cha = store.issue_at(b"addr-a", nonce(1), t0);
        store.issue_at(b"addr-b", nonce(2), t0);
        let v = StubVerifier { ok_challenge: cha, ok_sig: b"s".to_vec() };
        // A's challenge does not authorize B.
        assert_eq!(store.redeem_at(b"addr-b", FVK, b"s", &v, t0), Err(ClaimError::BadSignature));
        // A still redeems with its own challenge.
        assert_eq!(store.redeem_at(b"addr-a", FVK, b"s", &v, t0), Ok(cha));
    }
}
