//! Pool-wide aggregate handlers, served through the pool TTL cache.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use serde_json::Value;

use katpool_db::repo::{block, connection_session, payout, share_reject, share_stats, treasury};

use crate::error::ApiError;
use crate::handlers::{cached_json, resolve_window, to_value};
use crate::models::{
    ActiveMinersHistory, ActiveMinersPointView, ActiveSessionsView, BlockCounts, BlockView,
    BlocksPage, CycleDetailPage, CycleRecipientView, CycleView, CyclesPage, FirmwareBreakdown,
    FirmwareEntryView, GeoBreakdown, GeoEntryView, HashrateHistory, HashratePointView,
    HashrateSnapshot, MiningPoolStats, MpsBlock,
    PayoutTotals, PoolRejectsResponse, PoolStats, RejectReasonCount, TreasuryView,
};
use crate::money::KasAmount;
use crate::params::{self, PageParams, RangeParams, WindowParams};
use crate::state::AppState;

/// `GET /api/v1/pool/stats` — headline pool figures over a sliding window.
pub async fn stats(
    State(state): State<AppState>,
    Query(window_params): Query<WindowParams>,
) -> Result<Json<Arc<Value>>, ApiError> {
    let window = params::window(&window_params)?;
    let key = format!("pool/stats?w={}", window.as_secs());
    let cache = state.pool_cache.clone();
    cached_json(&cache, key, build_stats(state, window)).await
}

async fn build_stats(state: AppState, window: std::time::Duration) -> Result<Value, ApiError> {
    let w = resolve_window(window);

    let accepted = share_stats::accepted_pool_wide(&state.pool, w.since).await?;
    let hashrate_hs =
        share_stats::hashrate_estimate_pool_wide(&state.pool, w.since, w.until).await?;
    let counts = share_stats::active_participant_counts(&state.pool, w.since).await?;
    let block_rows = block::count_by_status(&state.pool).await?;
    let totals = payout::pool_payout_totals(&state.pool).await?;
    let treasury_snapshot = treasury::latest(&state.pool).await?;

    let resp = PoolStats {
        as_of: w.until,
        window_secs: w.secs,
        miners_active: counts.wallets,
        workers_active: counts.workers,
        hashrate_hs,
        accepted_shares: accepted.share_count,
        blocks: BlockCounts::from_rows(&block_rows),
        payouts: PayoutTotals {
            kas_confirmed: KasAmount::from_sompi(totals.kas_confirmed_sompi),
            nacho_confirmed: KasAmount::from_sompi(totals.nacho_confirmed_sompi),
            confirmed_payouts: totals.confirmed_payouts,
        },
        treasury: treasury_snapshot.map(|t| TreasuryView {
            captured_at: t.captured_at,
            kas_balance: KasAmount::from_sompi(t.kas_balance_sompi),
            nacho_balance: t.nacho_balance.to_string(),
            daa_score: t.daa_score,
            blue_score: t.blue_score,
        }),
    };
    to_value(&resp)
}

/// `GET /api/v1/pool/hashrate` — current pool hashrate estimate.
pub async fn hashrate(
    State(state): State<AppState>,
    Query(window_params): Query<WindowParams>,
) -> Result<Json<Arc<Value>>, ApiError> {
    let window = params::window(&window_params)?;
    let key = format!("pool/hashrate?w={}", window.as_secs());
    let cache = state.pool_cache.clone();
    cached_json(&cache, key, async move {
        let w = resolve_window(window);
        let hashrate_hs =
            share_stats::hashrate_estimate_pool_wide(&state.pool, w.since, w.until).await?;
        to_value(&HashrateSnapshot {
            hashrate_hs,
            window_secs: w.secs,
        })
    })
    .await
}

/// `GET /api/v1/pool/hashrate/history` — bucketed pool hashrate series.
pub async fn hashrate_history(
    State(state): State<AppState>,
    Query(range_params): Query<RangeParams>,
) -> Result<Json<Arc<Value>>, ApiError> {
    let range = params::range(&range_params)?;
    // Align the cache key to 10-second ticks so rapid dashboard polls hit the
    // same entry while the underlying series still uses the caller's `until`.
    let cache_until = range.until.timestamp().div_euclid(10) * 10;
    let key = format!(
        "pool/hashrate/history?from={}&to={}&b={}",
        range.from.timestamp(),
        cache_until,
        range.bucket.seconds()
    );
    let cache = state.pool_cache.clone();
    cached_json(&cache, key, async move {
        let points = share_stats::hashrate_series_pool_wide(
            &state.pool,
            range.from,
            range.until,
            range.bucket.seconds(),
        )
        .await?;
        to_value(&HashrateHistory {
            from: range.from,
            to: range.until,
            bucket: bucket_token(range.bucket),
            points: points
                .into_iter()
                .map(|p| HashratePointView {
                    bucket_start: p.bucket_start,
                    hashrate_hs: p.hashrate,
                    partial: p.is_partial,
                })
                .collect(),
        })
    })
    .await
}

/// `GET /api/v1/pool/blocks` — recent blocks, keyset-paginated.
pub async fn blocks(
    State(state): State<AppState>,
    Query(page_params): Query<PageParams>,
) -> Result<Json<Arc<Value>>, ApiError> {
    let page = params::page(&page_params)?;
    let key = format!("pool/blocks?l={}&before={:?}", page.limit, page.before_id);
    let cache = state.pool_cache.clone();
    cached_json(&cache, key, async move {
        let rows = block::list_recent(&state.pool, page.limit, page.before_id).await?;
        let next_before = next_cursor(rows.len(), page.limit, rows.last().map(|b| b.id.0));
        let blocks = rows.iter().map(BlockView::from).collect();
        to_value(&BlocksPage {
            blocks,
            next_before,
        })
    })
    .await
}

/// `GET /api/pool/miningPoolStats` — legacy-compatible `MiningPoolStats` feed.
///
/// Unversioned path matching the legacy pool **exactly** so the public
/// aggregator listing (miningpoolstats.stream) is uninterrupted by the
/// cutover. Composed from the same repo functions as the rest of the API.
pub async fn mining_pool_stats(
    State(state): State<AppState>,
) -> Result<Json<Arc<Value>>, ApiError> {
    let cache = state.pool_cache.clone();
    cached_json(
        &cache,
        "pool/miningPoolStats".to_owned(),
        build_mining_pool_stats(state),
    )
    .await
}

// `poolFee` is a fee percent derived from integer basis points, and the
// hashrate string is a base-1000 scale — both are display-only float math.
#[allow(clippy::float_arithmetic)]
async fn build_mining_pool_stats(state: AppState) -> Result<Value, ApiError> {
    let cfg = &state.config;

    // Pool hashrate over the same 10-minute window as `/pool/stats`.
    let now = chrono::Utc::now();
    let since = now - chrono::Duration::seconds(600);
    let hashrate_hs = share_stats::hashrate_estimate_pool_wide(&state.pool, since, now).await?;

    let recent = block::list_recent_with_identity(&state.pool, 100).await?;
    let total_blocks_count = block::total_count(&state.pool).await?;

    // Millisecond-precision UTC with a `Z` suffix, matching the legacy feed's
    // timestamp format byte-for-byte.
    let iso =
        |t: chrono::DateTime<chrono::Utc>| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

    let (lastblock, lastblocktime) = recent.first().map_or_else(
        || (String::new(), String::new()),
        |b| (hex::encode(&b.hash), iso(b.found_at)),
    );

    let top_100_blocks = recent
        .iter()
        .map(|b| MpsBlock {
            mined_block_hash: hex::encode(&b.hash),
            miner_id: b.worker_name.clone(),
            pool_address: cfg.mps_pool_address.clone(),
            reward_block_hash: String::new(),
            wallet: b.wallet_address.clone(),
            daa_score: b.daa_score,
            miner_reward: b.miner_reward_sompi.unwrap_or(0),
            timestamp: iso(b.found_at),
        })
        .collect();

    let resp = MiningPoolStats {
        coin_mined: "Kaspa".to_owned(),
        pool_name: cfg.mps_pool_name.clone(),
        url: cfg.mps_url.clone(),
        pool_fee: f64::from(cfg.mps_fee_bps) / 100.0,
        current_hash_rate: format_hashrate_compact(hashrate_hs),
        top_100_blocks,
        total_blocks_count,
        advertise_image_link: cfg.mps_ad_image_link.clone(),
        min_pay: cfg.mps_min_pay_kas,
        country: cfg.mps_country.clone(),
        fee_type: cfg.mps_fee_type.clone(),
        lastblock,
        lastblocktime,
    };
    to_value(&resp)
}

/// Format an H/s rate as a compact unit string with no space, e.g.
/// `766.99TH/s` — matching the legacy `MiningPoolStats` `current_hashRate`.
// Base-1000 scaling + a log are inherent float math for a display string.
#[allow(
    clippy::float_arithmetic,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::indexing_slicing
)]
fn format_hashrate_compact(hs: f64) -> String {
    const UNITS: [&str; 8] = [
        "H/s", "KH/s", "MH/s", "GH/s", "TH/s", "PH/s", "EH/s", "ZH/s",
    ];
    if !hs.is_finite() || hs <= 0.0 {
        return "0.00H/s".to_owned();
    }
    let exp = ((hs.log10() / 3.0).floor() as usize).min(UNITS.len() - 1);
    let scaled = hs / 1000_f64.powi(exp as i32);
    format!("{scaled:.2}{}", UNITS[exp])
}

/// `GET /api/v1/pool/payouts` — recent payout cycles, keyset-paginated.
pub async fn payouts(
    State(state): State<AppState>,
    Query(page_params): Query<PageParams>,
) -> Result<Json<Arc<Value>>, ApiError> {
    let page = params::page(&page_params)?;
    let key = format!("pool/payouts?l={}&before={:?}", page.limit, page.before_id);
    let cache = state.pool_cache.clone();
    cached_json(&cache, key, async move {
        let rows = payout::list_recent_cycles(&state.pool, page.limit, page.before_id).await?;
        let next_before = next_cursor(rows.len(), page.limit, rows.last().map(|c| c.id));
        let cycles = rows.iter().map(CycleView::from).collect();
        to_value(&CyclesPage {
            cycles,
            next_before,
        })
    })
    .await
}

/// `GET /api/v1/pool/payouts/:cycle_id` — one cycle with every recipient.
pub async fn payout_cycle(
    State(state): State<AppState>,
    Path(cycle_id): Path<i64>,
) -> Result<Json<Arc<Value>>, ApiError> {
    let key = format!("pool/payouts/{cycle_id}");
    let cache = state.pool_cache.clone();
    cached_json(&cache, key, async move {
        let cycle = payout::get_cycle(&state.pool, cycle_id).await?;
        let rows = payout::list_cycle_recipients(&state.pool, cycle_id).await?;
        let recipients = rows.iter().map(CycleRecipientView::from).collect();
        to_value(&CycleDetailPage {
            cycle: CycleView::from(&cycle),
            recipients,
        })
    })
    .await
}

// `GET /api/v1/pool/leaderboard` was intentionally REMOVED: ZKas does not
// expose per-miner or top-miner rankings (address, hashrate, pool-share) — that
// deanonymizes miners. Only aggregate pool figures are served. See `api::app`.

/// `GET /api/v1/pool/miners/history` — active-miner count over time.
pub async fn active_miners_history(
    State(state): State<AppState>,
    Query(range_params): Query<RangeParams>,
) -> Result<Json<Arc<Value>>, ApiError> {
    let range = params::range(&range_params)?;
    let key = format!(
        "pool/miners/history?from={}&to={}&b={}",
        range.from.timestamp(),
        range.until.timestamp(),
        range.bucket.seconds()
    );
    let cache = state.pool_cache.clone();
    cached_json(&cache, key, async move {
        let points = share_stats::active_wallets_series(
            &state.pool,
            range.from,
            range.until,
            range.bucket.seconds(),
        )
        .await?;
        to_value(&ActiveMinersHistory {
            from: range.from,
            to: range.until,
            bucket: bucket_token(range.bucket),
            points: points
                .into_iter()
                .map(|p| ActiveMinersPointView {
                    bucket_start: p.bucket_start,
                    miners: p.miners,
                })
                .collect(),
        })
    })
    .await
}

/// `GET /api/v1/pool/firmware` — miner-software breakdown over a window.
pub async fn firmware(
    State(state): State<AppState>,
    Query(window_params): Query<WindowParams>,
) -> Result<Json<Arc<Value>>, ApiError> {
    let window = params::window(&window_params)?;
    let key = format!("pool/firmware?w={}", window.as_secs());
    let cache = state.pool_cache.clone();
    cached_json(&cache, key, async move {
        let w = resolve_window(window);
        let rows = connection_session::firmware_breakdown(&state.pool, w.since).await?;
        to_value(&FirmwareBreakdown {
            window_secs: w.secs,
            entries: rows
                .into_iter()
                .map(|r| FirmwareEntryView {
                    app: r.remote_app,
                    workers: r.workers,
                    sessions: r.sessions,
                })
                .collect(),
        })
    })
    .await
}

/// `GET /api/v1/pool/geo` — aggregate miner country distribution.
///
/// Aggregates resolved session countries over a sliding window
/// (ADR-0025). Aggregate-only: no IP, no per-miner geo. Country comes
/// from `MaxMind` `GeoLite2` (attribution required). Returns an empty
/// `entries` array when geo resolution is disabled or unpopulated.
pub async fn geo(
    State(state): State<AppState>,
    Query(window_params): Query<WindowParams>,
) -> Result<Json<Arc<Value>>, ApiError> {
    let window = params::window(&window_params)?;
    let key = format!("pool/geo?w={}", window.as_secs());
    let cache = state.pool_cache.clone();
    cached_json(&cache, key, async move {
        let w = resolve_window(window);
        let rows = connection_session::country_breakdown(&state.pool, w.since).await?;
        to_value(&GeoBreakdown {
            window_secs: w.secs,
            entries: rows
                .into_iter()
                .map(|r| GeoEntryView {
                    country: r.country,
                    workers: r.workers,
                    sessions: r.sessions,
                })
                .collect(),
        })
    })
    .await
}

/// `GET /api/v1/pool/active-sessions` — live "connected now" snapshot.
///
/// Counts currently-open stratum sessions and the distinct authenticated
/// workers among them (B1 session lifecycle). Aggregate-only: no IP, no
/// per-miner identity. Short-TTL cached like the other pool aggregates.
pub async fn active_sessions(State(state): State<AppState>) -> Result<Json<Arc<Value>>, ApiError> {
    let key = "pool/active-sessions".to_string();
    let cache = state.pool_cache.clone();
    cached_json(&cache, key, async move {
        let s = connection_session::active_summary(&state.pool).await?;
        to_value(&ActiveSessionsView {
            active_sessions: s.sessions,
            active_workers: s.workers,
        })
    })
    .await
}

/// `GET /api/v1/pool/rejects` — pool-wide reject breakdown by reason.
///
/// Aggregates `share_reject` across all wallets over a sliding window,
/// mirroring the per-miner `rejects` surface. Backs the operator
/// anti-abuse view: which reject reasons dominate pool-wide, right now.
pub async fn rejects(
    State(state): State<AppState>,
    Query(window_params): Query<WindowParams>,
) -> Result<Json<Arc<Value>>, ApiError> {
    let window = params::window(&window_params)?;
    let key = format!("pool/rejects?w={}", window.as_secs());
    let cache = state.pool_cache.clone();
    cached_json(&cache, key, async move {
        let w = resolve_window(window);
        let rows = share_reject::count_by_reason_pool_wide(&state.pool, w.since).await?;
        let total: i64 = rows.iter().map(|(_, count)| *count).sum();
        let by_reason = rows
            .into_iter()
            .map(|(reason, count)| RejectReasonCount::from_row(reason, count))
            .collect();
        to_value(&PoolRejectsResponse {
            window_secs: w.secs,
            total,
            by_reason,
        })
    })
    .await
}

/// The wire token for a bucket width.
pub(crate) const fn bucket_token(bucket: params::Bucket) -> &'static str {
    match bucket {
        params::Bucket::OneMinute => "1m",
        params::Bucket::FiveMinutes => "5m",
        params::Bucket::OneHour => "1h",
        params::Bucket::OneDay => "1d",
    }
}

/// The next keyset cursor: `Some(last_id)` only when the page was full
/// (so there may be more), else `None`.
pub(crate) fn next_cursor(returned: usize, limit: i64, last_id: Option<i64>) -> Option<i64> {
    if i64::try_from(returned).is_ok_and(|n| n >= limit) {
        last_id
    } else {
        None
    }
}
