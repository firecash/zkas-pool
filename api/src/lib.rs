//! Public **read-only** HTTP API for the katpool unified runtime (ADR-0021).
//!
//! Embedded in the `katpool` binary as an env-gated task on
//! `KATPOOL_API_PORT`. It exposes three unversioned liveness/readiness probes
//! and a versioned `/api/v1` data surface composed entirely from
//! `katpool-db` repo functions:
//!
//! - `GET /health` / `/ready` / `/started` — liveness / readiness / startup.
//! - `GET /api/v1/pool/{stats,hashrate,hashrate/history,blocks,payouts,payouts/:id}`.
//! - `GET /api/v1/pool/{leaderboard,miners/history,firmware,rejects,geo}`.
//! - `GET /api/v1/pool/active-sessions` — live connected-now snapshot.
//! - `GET /api/v1/balance/{address}`.
//! - `GET /api/v1/miners/{address}`, `.../workers`, `.../hashrate/history`,
//!   `.../payouts`, `.../rejects`.
//! - `GET /api/v1/full_rebate/{address}`.
//!
//! It holds **no funds and no secrets**: it never imports the payout/signing
//! crates and reads `PostgreSQL` only. The edge is per-IP rate-limited
//! (`tower_governor`), body-bounded, hard-timed-out, and TTL-cached
//! (`moka`); on-chain amounts serialize as decimal strings and addresses are
//! redacted in telemetry.

#![cfg_attr(not(test), warn(missing_docs))]

pub mod claim;
pub mod config;
pub mod error;
pub mod handlers;
pub mod models;
pub mod money;
pub mod params;
pub mod redact;
pub mod state;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::{HeaderValue, Method, Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::routing::get;
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tower_governor::GovernorLayer;
use tower_governor::governor::GovernorConfigBuilder;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

pub use crate::config::ApiConfig;
use crate::config::MAX_BODY_BYTES;
pub use crate::error::ApiError;
pub use crate::state::{AppState, ReadinessHandle};

/// Crate version constant.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// How often the background DB readiness probe runs.
const DB_PROBE_INTERVAL: Duration = Duration::from_secs(5);

/// How often the rate-limiter's per-IP storage is garbage-collected.
const GOVERNOR_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// Build the application router with its cheap middleware stack.
///
/// Routes, state, body limit, hard timeout, tracing, and optional CORS. The
/// per-IP rate limiter is **not** applied here — it needs peer-IP connection
/// info and is added by [`serve`]. Exposed for tests, which exercise the
/// router directly via `tower::ServiceExt::oneshot`.
pub fn app(state: AppState) -> Router {
    let config = Arc::clone(&state.config);

    let v1 = Router::new()
        .route("/pool/stats", get(handlers::pool::stats))
        .route("/pool/hashrate", get(handlers::pool::hashrate))
        .route(
            "/pool/hashrate/history",
            get(handlers::pool::hashrate_history),
        )
        .route("/pool/blocks", get(handlers::pool::blocks))
        .route("/pool/payouts", get(handlers::pool::payouts))
        .route(
            "/pool/payouts/{cycle_id}",
            get(handlers::pool::payout_cycle),
        )
        // NOTE: /pool/leaderboard (top-miner ranking) is intentionally removed —
        // ZKas does not expose per-miner or top-miner stats (miner privacy).
        // Only aggregate pool figures are served.
        .route(
            "/pool/miners/history",
            get(handlers::pool::active_miners_history),
        )
        .route("/pool/firmware", get(handlers::pool::firmware))
        .route("/pool/rejects", get(handlers::pool::rejects))
        .route("/pool/geo", get(handlers::pool::geo))
        .route(
            "/pool/active-sessions",
            get(handlers::pool::active_sessions),
        )
        .route("/balance/{address}", get(handlers::miner::balance))
        .route("/miners/{address}", get(handlers::miner::profile))
        .route("/miners/{address}/workers", get(handlers::miner::workers))
        .route(
            "/miners/{address}/hashrate/history",
            get(handlers::miner::hashrate_history),
        )
        .route("/miners/{address}/payouts", get(handlers::miner::payouts))
        .route("/miners/{address}/rejects", get(handlers::miner::rejects))
        .route("/full_rebate/{address}", get(handlers::miner::full_rebate));

    let router = Router::new()
        .route("/health", get(handlers::health::health))
        .route("/ready", get(handlers::health::ready))
        .route("/started", get(handlers::health::started))
        // Legacy-compatible aggregator feed at the EXACT unversioned path the
        // legacy pool served, so the miningpoolstats.stream listing survives
        // the cutover unchanged (ADR-0021 keeps the rest under /api/v1).
        .route(
            "/api/pool/miningPoolStats",
            get(handlers::pool::mining_pool_stats),
        )
        .nest("/api/v1", v1)
        .with_state(state)
        .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::SERVICE_UNAVAILABLE,
            config.request_timeout,
        ))
        // Per-request span at INFO so it survives the `info` env-filter and is
        // exported to Tempo. Only the method is recorded — the URI path carries
        // miner addresses, which we keep out of telemetry (see `redact`).
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::extract::Request| {
                tracing::info_span!("http.request", method = %request.method())
            }),
        )
        .layer(middleware::from_fn(api_no_store));

    if let Some(cors) = cors_layer(config.cors_allow_origin.as_deref()) {
        router.layer(cors)
    } else {
        router
    }
}

/// Build a router exposing **only** the unversioned liveness/readiness probes
/// (`/health`, `/ready`, `/started`).
///
/// This is the orchestrator-facing surface (systemd/Railway/k8s), deliberately
/// decoupled from the public data API: no rate limiter, no CORS, no cache, no
/// `/api/v1`. It reuses the same handlers and [`ReadinessHandle`] as [`app`],
/// so a dedicated health port reports the *same* DB-reachable + kaspad-synced
/// readiness as the API does — with or without `KATPOOL_API_PORT` enabled.
pub fn health_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handlers::health::health))
        .route("/ready", get(handlers::health::ready))
        .route("/started", get(handlers::health::started))
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}

/// Prevent shared caches (CDN, reverse proxy) from storing dynamic API JSON.
async fn api_no_store(request: Request<axum::body::Body>, next: Next) -> axum::response::Response {
    let no_store = request.uri().path().starts_with("/api/");
    let mut response = next.run(request).await;
    if no_store {
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("private, no-store, no-cache, must-revalidate"),
        );
    }
    response
}

/// Build a read-only CORS layer for an explicit origin, or `None` to install
/// no CORS layer (same-origin only). A malformed origin disables CORS with a
/// loud log rather than failing startup.
fn cors_layer(origin: Option<&str>) -> Option<CorsLayer> {
    let origin = origin?;
    match origin.parse::<HeaderValue>() {
        Ok(value) => Some(
            CorsLayer::new()
                .allow_methods([Method::GET])
                .allow_headers([header::ACCEPT, header::CONTENT_TYPE])
                .allow_origin(value),
        ),
        Err(err) => {
            tracing::error!(%origin, error = %err, "invalid KATPOOL_API_CORS_ALLOW_ORIGIN; CORS disabled");
            None
        }
    }
}

/// Token-replenish interval for a sustained rate, in requests per second.
///
/// `tower_governor`'s `GovernorConfigBuilder::per_second(n)` is a foot-gun: it
/// sets the replenish *period* to `n` seconds (one token every `n`s, i.e.
/// `1/n` req/s) — the inverse of what the name implies. Sizing the limiter by
/// that method throttles to a trickle (e.g. `per_second(100)` ⇒ one token every
/// 100s). We instead derive the period from true throughput: one token every
/// `1s / rate`, so `rate` tokens accrue per second with `burst_size` capacity.
fn replenish_period(rate_per_second: u64) -> Duration {
    // `rate_per_second` is validated > 0 (`ApiConfig::validate`). Clamp into the
    // u32 that `Duration / u32` needs; a rate above u32::MAX is nonsensical for
    // a read-only API, and `.max(1)` keeps the period non-zero so `finish()`
    // never silently drops the limiter.
    let rate = u32::try_from(rate_per_second).unwrap_or(u32::MAX).max(1);
    // Floor at 1ns: for rate > 1e9 the integer `Duration / u32` truncates to
    // zero, which `finish()` treats as "no limiter".
    (Duration::from_secs(1) / rate).max(Duration::from_nanos(1))
}

/// Serve the API on an already-bound listener until the process exits.
///
/// Wraps [`app`] with the per-IP rate limiter and serves with
/// per-connection peer-IP info (required by `PeerIpKeyExtractor`). Spawns a
/// background task to GC the limiter's storage.
///
/// # Errors
/// Propagates any fatal `axum::serve` I/O error.
pub async fn serve(listener: tokio::net::TcpListener, state: AppState) -> std::io::Result<()> {
    let config = Arc::clone(&state.config);
    let router = app(state);

    let governor_conf = GovernorConfigBuilder::default()
        .period(replenish_period(config.rate_per_second))
        .burst_size(config.rate_burst)
        .finish();

    let governed = if let Some(conf) = governor_conf {
        let conf = Arc::new(conf);
        // GC the limiter's per-IP storage so memory stays bounded.
        let limiter = conf.limiter().clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(GOVERNOR_CLEANUP_INTERVAL);
            loop {
                ticker.tick().await;
                limiter.retain_recent();
            }
        });
        router.layer(GovernorLayer::new(conf))
    } else {
        tracing::error!("rate-limiter config invalid; serving WITHOUT in-app rate limiting");
        router
    };

    axum::serve(
        listener,
        governed.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}

/// Bind `addr` and serve. Convenience wrapper used by the runtime.
///
/// # Errors
/// Returns the bind error if the address is unavailable, or any fatal serve
/// error thereafter.
pub async fn serve_on(addr: SocketAddr, state: AppState) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "katpool public API listening");
    serve(listener, state).await
}

/// Bind `addr` and serve only the liveness/readiness probes ([`health_router`])
/// until the process exits.
///
/// Used by the runtime to expose health on a dedicated port
/// (`KATPOOL_HEALTH_CHECK_PORT`) independent of the public data API. No rate
/// limiter or connect-info is installed — orchestrator probes must never be
/// throttled and carry no per-IP semantics.
///
/// # Errors
/// Returns the bind error if the address is unavailable, or any fatal serve
/// error thereafter.
pub async fn serve_health_on(addr: SocketAddr, state: AppState) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "katpool health-check endpoint listening");
    axum::serve(listener, health_router(state).into_make_service()).await
}

/// Spawn the background database-reachability probe.
///
/// Probes every `DB_PROBE_INTERVAL` and updates the readiness flag. The
/// runtime owns the kaspad-sync and startup flags (driven by its maturity
/// poller). The returned handle may be dropped; the task runs until abort.
pub fn spawn_db_readiness_probe(pool: PgPool, readiness: ReadinessHandle) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(DB_PROBE_INTERVAL);
        loop {
            ticker.tick().await;
            let ok = sqlx::query("SELECT 1").execute(&pool).await.is_ok();
            readiness.set_db_reachable(ok);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::replenish_period;
    use std::time::Duration;

    #[test]
    fn replenish_period_is_the_inverse_of_rate() {
        // rate (req/s) -> one token every (1s / rate)
        assert_eq!(replenish_period(1), Duration::from_secs(1));
        assert_eq!(replenish_period(5), Duration::from_millis(200));
        assert_eq!(replenish_period(100), Duration::from_millis(10));
        assert_eq!(replenish_period(1000), Duration::from_millis(1));
    }

    #[test]
    fn replenish_period_never_zero() {
        // Even an absurd rate must keep the period > 0 so `finish()` keeps the
        // limiter installed rather than silently disabling it.
        assert!(replenish_period(u64::MAX) > Duration::ZERO);
    }
}
