//! Deterministic payout-cycle DAA windowing.
//!
//! The engine has no wall clock it can trust across instances, so the cycle
//! identity is derived from the chain's virtual DAA score instead. Bucketing
//! the score by a fixed `span` makes the `(daa_start, daa_end)` window — and
//! therefore the cycle's idempotency key `zkas-{start}-{end}` — stable for the
//! whole span: every tick inside one bucket resumes the *same* cycle, and
//! crossing a boundary opens the next one. `span` sets the cadence (≈ seconds
//! at Kaspa's ~1 block/sec, so `86_400` ≈ daily).

use katpool_domain::DaaScore;

/// Map the current virtual DAA score to a stable half-open `[start, end)`
/// payout window of width `span` (clamped to ≥ 1).
// Integer (floor) division is the whole point: it assigns the score to a fixed
// bucket so the window — and the cycle's idempotency key — is stable for the span.
#[allow(clippy::integer_division)]
#[must_use]
pub fn cycle_window(virtual_daa_score: u64, span: u64) -> (DaaScore, DaaScore) {
    let span = span.max(1);
    let bucket = virtual_daa_score / span;
    let start = bucket.saturating_mul(span);
    let end = start.saturating_add(span);
    (DaaScore::new(start), DaaScore::new(end))
}

#[cfg(test)]
mod tests {
    use super::cycle_window;

    #[test]
    fn window_is_stable_within_a_bucket() {
        let span = 1_000;
        let (a_start, a_end) = cycle_window(5_000, span);
        let (b_start, b_end) = cycle_window(5_999, span);
        assert_eq!(a_start.value(), 5_000);
        assert_eq!(a_end.value(), 6_000);
        assert_eq!(
            (a_start.value(), a_end.value()),
            (b_start.value(), b_end.value())
        );
    }

    #[test]
    fn crossing_a_boundary_opens_the_next_window() {
        let span = 1_000;
        let (_, prev_end) = cycle_window(5_999, span);
        let (next_start, next_end) = cycle_window(6_000, span);
        assert_eq!(next_start.value(), prev_end.value());
        assert_eq!(next_start.value(), 6_000);
        assert_eq!(next_end.value(), 7_000);
    }

    #[test]
    fn zero_span_is_clamped_and_does_not_panic() {
        let (start, end) = cycle_window(42, 0);
        assert_eq!(start.value(), 42);
        assert_eq!(end.value(), 43);
    }
}
