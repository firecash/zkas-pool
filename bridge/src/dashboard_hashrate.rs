//! Bounded, difficulty-weighted work in a rolling five-minute window.
//! The sampling clock belongs to the process, not to a reconnecting worker.
use std::collections::HashMap;

pub const WINDOW_SECONDS: u64 = 300;
const BUCKETS: usize = WINDOW_SECONDS as usize;

#[derive(Clone, Copy, Default)]
struct Bucket {
    second: u64,
    work_gh: f64,
}

struct WorkerWindow {
    buckets: Box<[Bucket; BUCKETS]>,
    last_second: u64,
}

pub struct RollingHashrate {
    workers: HashMap<String, WorkerWindow>,
    capacity: usize,
    cleaned_at: u64,
}

impl RollingHashrate {
    pub fn new(capacity: usize) -> Self {
        Self { workers: HashMap::new(), capacity: capacity.max(1), cleaned_at: 0 }
    }

    pub fn record(&mut self, key: String, seconds: f64, work_gh: f64) {
        if !seconds.is_finite() || seconds < 0.0 || !work_gh.is_finite() || work_gh <= 0.0 {
            return;
        }
        let second = seconds.floor() as u64;
        if second.saturating_sub(self.cleaned_at) >= 5 {
            self.workers.retain(|_, w| second.saturating_sub(w.last_second) < WINDOW_SECONDS);
            self.cleaned_at = second;
        }
        if !self.workers.contains_key(&key) && self.workers.len() >= self.capacity {
            if let Some(oldest) = self.workers.iter().min_by_key(|(_, w)| w.last_second).map(|(k, _)| k.clone()) {
                self.workers.remove(&oldest);
            }
        }
        let worker = self
            .workers
            .entry(key)
            .or_insert_with(|| WorkerWindow { buckets: Box::new([Bucket::default(); BUCKETS]), last_second: second });
        let bucket = &mut worker.buckets[(second % WINDOW_SECONDS) as usize];
        if bucket.second != second {
            *bucket = Bucket { second, work_gh: 0.0 };
        }
        bucket.work_gh += work_gh;
        worker.last_second = second;
    }

    pub fn rates(&self, seconds: f64) -> HashMap<String, f64> {
        if !seconds.is_finite() || seconds < 0.0 {
            return HashMap::new();
        }
        let second = seconds.floor() as u64;
        let elapsed = seconds.clamp(1.0, WINDOW_SECONDS as f64);
        self.workers
            .iter()
            .filter_map(|(key, worker)| {
                let work: f64 = worker
                    .buckets
                    .iter()
                    .filter(|b| b.second <= second && second - b.second < WINDOW_SECONDS)
                    .map(|b| b.work_gh)
                    .sum();
                (work > 0.0).then(|| (key.clone(), work / elapsed))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downtime_before_window_does_not_dilute_current_rate() {
        let mut rates = RollingHashrate::new(10);
        rates.record("old".into(), 0.0, 1.0);
        for second in 3600..3900 {
            rates.record("rig".into(), second as f64, 300_000.0);
        }
        assert_eq!(rates.rates(3899.9)["rig"], 300_000.0);
        assert!(!rates.rates(3899.9).contains_key("old"));
    }

    #[test]
    fn rotating_worker_names_do_not_reduce_or_inflate_pool_hashrate() {
        let mut rates = RollingHashrate::new(100);
        for second in 1000..1300 {
            rates.record(format!("worker-{}", (second - 1000) / 10), second as f64, 500_000.0);
        }
        assert!((rates.rates(1299.9).values().sum::<f64>() - 500_000.0).abs() < 1e-6);
    }

    #[test]
    fn expiration_wrap_and_zero_activity_are_exact_at_second_boundary() {
        let mut rates = RollingHashrate::new(10);
        rates.record("rig".into(), 0.0, 300.0);
        assert_eq!(rates.rates(299.0)["rig"], 300.0 / 299.0);
        assert!(rates.rates(300.0).is_empty());
        rates.record("rig".into(), 300.0, 600.0);
        assert_eq!(rates.rates(300.0)["rig"], 2.0);
        assert!(rates.rates(600.0).is_empty());
    }

    #[test]
    fn mixed_difficulty_is_credited_by_actual_work_not_share_count() {
        let mut rates = RollingHashrate::new(10);
        rates.record("rig".into(), 1.0, 10.0);
        rates.record("rig".into(), 1.5, 90.0);
        assert_eq!(rates.rates(10.0)["rig"], 10.0);
    }

    #[test]
    fn startup_denominator_is_shared_and_finite() {
        let mut rates = RollingHashrate::new(10);
        rates.record("a".into(), 0.0, 10.0);
        rates.record("b".into(), 9.0, 90.0);
        assert_eq!(rates.rates(10.0).values().sum::<f64>(), 10.0);
        assert_eq!(rates.rates(0.0)["a"], 10.0);
    }

    #[test]
    fn invalid_work_is_ignored_and_cardinality_is_bounded() {
        let mut rates = RollingHashrate::new(2);
        for bad in [f64::NAN, f64::INFINITY, -1.0, 0.0] {
            rates.record("bad".into(), 0.0, bad);
        }
        assert!(rates.rates(0.0).is_empty());
        rates.record("a".into(), 1.0, 10.0);
        rates.record("b".into(), 2.0, 10.0);
        rates.record("c".into(), 3.0, 10.0);
        assert_eq!(rates.workers.len(), 2);
        assert!(!rates.rates(3.0).contains_key("a"));
    }
}
