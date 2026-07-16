//! katpool — Phase 7 wiring binary.
//!
//! As of Phase 3 M3d the binary composes the **stratum bridge**
//! plus the **accountant**'s event consumer and maturity tracker
//! into a single process with a shared `broadcast::Sender<PoolEvent>`
//! channel. Phase 4 adds payout-kas, Phase 5 payout-krc20,
//! Phase 6 the read-only API, Phase 7 closes out telemetry/
//! secrets/config wiring.
//!
//! ## Subsystems
//!
//! 1. **Bridge stratum server**. Listens on `KATPOOL_STRATUM_PORT`.
//!    Talks to kaspad via the bridge's own `KaspaApi` (block
//!    template fetch + submission). Emits `PoolEvent` into the
//!    shared broadcast channel.
//! 2. **Accountant event consumer**. Drains the broadcast channel,
//!    writes share / block rows via the new schema's repo layer.
//! 3. **Maturity tracker**. Polls kaspad (via the same gRPC URL,
//!    separate client): resolves `submitted_to_node` blocks to
//!    `confirmed_blue` / `orphaned` by GHOSTDAG colour, and allocates
//!    matured coinbase UTXOs credited to the pool address via the
//!    accountant's allocation engine.
//!
//! All three subsystems shut down cleanly on SIGINT / SIGTERM via
//! a `tokio::sync::watch::Receiver<bool>` propagated from the
//! signal task.
//!
//! ## Commands
//!
//! Invoked with no arguments the binary runs the full daemon above. It also
//! accepts an operator on-demand payout subcommand:
//!
//! - `katpool payout run-now [--dry-run]` — drive a single KAS payout cycle
//!   synchronously (plan → broadcast → confirm → reconcile), then exit. It
//!   reads the same environment configuration as the daemon (including
//!   `KATPOOL_PAYOUT_DRY_RUN`) and acquires the shared `treasury:spend-leader`
//!   advisory lock, so it is safe to run while the daemon is live — only one
//!   treasury spender (KAS, KRC-20, or consolidation) acts at a time.
//!   `--dry-run` forces sign+verify without
//!   broadcasting regardless of the env setting.
//! - `katpool --help` — print usage.
//!
//! ## Configuration (environment variables + optional YAML/TOML file)
//!
//! Configuration is resolved with a strict, explicit precedence:
//!
//! ```text
//! environment variable  >  config file  >  built-in default
//! ```
//!
//! - `KATPOOL_CONFIG`                optional path to a YAML or TOML file
//!   (format inferred from the extension) parsed + validated by the
//!   `katpool-config` crate. It supplies values for the *core* keys below
//!   (node/db/network, stratum, maturity, and the operational toggles) only
//!   where the corresponding environment variable is unset; an env var always
//!   wins. Unknown keys and out-of-range values abort the boot. Payout,
//!   KRC-20, consolidation, and treasury-key settings remain environment-only
//!   by design (secrets / money-movement policy). Unset/empty ⇒ pure-env
//!   behavior, byte-for-byte unchanged. The file keys mirror the `KATPOOL_*`
//!   names lowercased (e.g. `KATPOOL_STRATUM_PORT` ⇒ `stratum_port`,
//!   `KATPOOL_API_PORT` ⇒ `api_port`, `KATPOOL_MATURITY_POLL_SECS` ⇒
//!   `maturity_poll_secs`); see `ops/config/katpool.example.yaml`.
//!
//! Required (env var or config-file key):
//! - `KASPAD_GRPC_URL`               (e.g. `grpc://127.0.0.1:16210`)
//! - `KATPOOL_DATABASE_URL`          postgres URL
//! - `KATPOOL_POOL_ADDRESS`          kaspa address(es), comma-separated
//!   (coinbase outputs to these become pool revenue)
//! - `KATPOOL_STRATUM_PORT`          e.g. `5555`
//!
//! Optional:
//! - `KATPOOL_STRATUM_PORTS`         multi-port binding with per-port
//!   starting-difficulty seeds (ADR-0022), `port:seed` comma-separated,
//!   e.g. `1111:256,2222:1024,3333:4096,4444:8192,5555:16384,6666:32768,7777:65536,8888:2048`.
//!   Each seed is the *initial* difficulty for that port; vardiff moves
//!   freely from there. Empty/unset => bind only `KATPOOL_STRATUM_PORT`.
//! - `KATPOOL_INSTANCE_ID`           default `katpool-runtime`
//! - `KATPOOL_FEE_TOPLINE_BPS`       default 75
//! - `KATPOOL_MIN_SHARE_DIFF`        default 4096 (ASIC-class floor;
//!   raise for higher-hashrate fleets, lower only for CPU/dev miners)
//! - `KATPOOL_VAR_DIFF`              default `true` (variable difficulty
//!   retargeting; set `false` to pin every miner at `min_share_diff`)
//! - `KATPOOL_SHARES_PER_MIN`        default 20 (vardiff retarget setpoint;
//!   ignored when `KATPOOL_VAR_DIFF=false`)
//! - `KATPOOL_STRATUM_PROXY_PROTOCOL` default `false`. When `true`, every
//!   stratum connection must begin with a PROXY protocol v2 header and the
//!   real miner IP is parsed from it (ADR-0022, fly.io edge). Enable only
//!   when fronted by the trusted forwarder with the origin stratum ports
//!   firewalled to the forwarder egress.
//! - `KATPOOL_PROM_PORT`             default empty (disabled)
//! - `KATPOOL_HEALTH_CHECK_PORT`     bind address (`host:port` or `:port`);
//!   empty = disabled. Serves a dedicated liveness/readiness surface
//!   (`/health` `/ready` `/started` only) using the same readiness source as
//!   the API, so orchestrators can probe even when `KATPOOL_API_PORT` is off
//!   (ADR-0021). The legacy `BridgeServerConfig.health_check_port` field still
//!   carries the raw value for the standalone bridge binary but is unused here.
//! - `KATPOOL_MATURITY_POLL_SECS`    default 15
//! - `KATPOOL_COINBASE_MATURITY`     default 1000 (DAA-score depth)
//! - `KATPOOL_WINDOW_DAA_SPAN`       default 600
//! - `KATPOOL_COINBASE_MIN_DAA_SCORE` default 0 (disabled). Cutover floor:
//!   ignore coinbase UTXOs below this DAA score so a prior pool's historical
//!   coinbases on a shared treasury address are never re-discovered.
//! - `KATPOOL_BROADCAST_CAPACITY`    default 4096
//! - `KATPOOL_GEOIP_DB`              optional `GeoLite2`/`GeoIP2` Country `.mmdb`
//!   path (ADR-0025). When set + loadable, sessions are tagged with an
//!   ISO-3166 country for the aggregate `/api/v1/pool/geo` view; unset or
//!   unreadable ⇒ geo disabled (NULL country), non-fatal.
//! - `KATPOOL_EVENT_RECORD_PATH`     optional NDJSON `PoolEvent` capture
//!   for offline replay via `accountant::replay`
//! - `KATPOOL_TIER_CLASSIFIER`       `static` (default) or `kasplex`. `static`
//!   marks every wallet `Standard` (the NACHO Elite rebate is inert);
//!   `kasplex` resolves on-chain NACHO holdings via the (mainnet-only) kasplex
//!   indexers, with a safe `Standard` fallback and an upstream circuit breaker
//!   (ADR-0012). Set `kasplex` on mainnet to activate the Elite rebate.
//! - `KATPOOL_SHUTDOWN_DRAIN_SECS`   default 10. Hard ceiling (seconds) on the
//!   event-backlog drain at SIGTERM: the consumer persists everything already
//!   on the bus before exiting, bounded by this budget.
//!
//! Telemetry (B1/B2; installed before config load so even a bad config logs):
//! - `KATPOOL_LOG_FORMAT`            `text` (default) or `json`. `text` is the
//!   `journalctl`-friendly single line; `json` emits one structured object per
//!   event for Loki ingestion (recommended once log shipping exists). An
//!   unrecognised value falls back to `text` rather than failing to boot.
//! - `KATPOOL_OTLP_ENDPOINT`         OTLP/gRPC collector endpoint (e.g.
//!   `http://tempo:4317`) for distributed-trace export (ADR-0004). Empty/unset
//!   disables span export entirely (the default until the LGTM stack exists).
//! - `OTEL_SERVICE_NAME`             overrides the exported `service.name`
//!   (defaults to `KATPOOL_INSTANCE_ID`).
//!
//! Treasury and wallet addresses are redacted to a `prefix:…last4` tag in all
//! logs/traces (`katpool_domain::redact`); treasury key material is structurally
//! unloggable (`katpool_secrets::TreasurySecret`).
//!
//! Public read-only HTTP API (Phase 6 — opt-in, ADR-0021):
//! - `KATPOOL_API_PORT`              bind address `host:port` (e.g.
//!   `127.0.0.1:8080`) or `:port` (all interfaces); empty = disabled. Serves
//!   the unversioned `/health`
//!   `/ready` `/started` probes plus the versioned `/api/v1` read-only data
//!   surface. `/ready` is DB-reachable AND kaspad-synced; the kaspad-sync
//!   signal reuses the maturity tracker's existing poll (no second gRPC
//!   connection), and `/started` latches once the first sweep observes it.
//! - `KATPOOL_API_RATE_PER_SECOND`   default 5  (per-IP sustained refill)
//! - `KATPOOL_API_RATE_BURST`        default 20 (per-IP burst capacity)
//! - `KATPOOL_API_REQUEST_TIMEOUT_SECS`  default 10
//! - `KATPOOL_API_POOL_CACHE_TTL_SECS`   default 10
//! - `KATPOOL_API_WALLET_CACHE_TTL_SECS` default 5
//! - `KATPOOL_API_CORS_ALLOW_ORIGIN` default empty (no CORS layer installed)
//!
//! KAS payout engine (M4.7 — opt-in, dry-run by default):
//! - `KATPOOL_PAYOUT_ENABLED`        default `false` (engine off)
//! - `KATPOOL_PAYOUT_DRY_RUN`        default `true` (sign+verify only;
//!   set `false` to broadcast real transactions)
//! - `KATPOOL_PAYOUT_POLL_SECS`      default 60
//! - `KATPOOL_PAYOUT_CYCLE_SPAN_DAA` default `216_000` (~6h at 10 BPS;
//!   block-rate-specific, must exceed the confirmation depth)
//! - `KATPOOL_PAYOUT_THRESHOLD_SOMPI` default 10 KAS
//! - `KATPOOL_PAYOUT_MAX_SOMPI_PER_CYCLE` optional per-cycle KAS spend cap
//!   (sompi). Unset = disabled. When set, a cycle whose total non-failed
//!   outbound exceeds it is refused before any broadcast — a money-safety
//!   circuit breaker (G1). Set a sane ceiling on mainnet.
//! - Treasury key source (one of, in precedence order):
//!   `KATPOOL_TREASURY_KEY_PATH` (raw 32-byte hex file, testnet
//!   rehearsal) else `KATPOOL_TREASURY_CREDENTIAL` (systemd
//!   `LoadCredentialEncrypted` name, default `treasury-key`).
//!   The treasury address is the first `KATPOOL_POOL_ADDRESS`.
//!
//! KRC-20 NACHO payout engine (M5.5b — opt-in, dry-run by default; shares
//! the treasury key/address and kaspad node, separate advisory-lock leader):
//! - `KATPOOL_KRC20_PAYOUT_ENABLED`        default `false` (engine off)
//! - `KATPOOL_KRC20_PAYOUT_DRY_RUN`        default `true` (settle records +
//!   broadcasts nothing; never credits)
//! - `KATPOOL_KRC20_PAYOUT_POLL_SECS`      default 60
//! - `KATPOOL_KRC20_PAYOUT_CYCLE_SPAN_DAA` default `216_000` (~6h at 10 BPS;
//!   block-rate-specific, must exceed the confirmation depth)
//! - `KATPOOL_KRC20_MIN_PENDING_SOMPI`     default 10 KAS (coarse pre-filter)
//! - `KATPOOL_KRC20_MIN_NACHO_BASE_UNITS`  default 1 NACHO (dust gate)
//! - `KATPOOL_KRC20_MAX_NACHO_PER_CYCLE`   optional per-cycle NACHO spend cap
//!   (base units). Unset = disabled. When set, a cycle whose total non-failed
//!   NACHO exceeds it is refused before any settle — the money-safety guard
//!   against a poisoned floor-price quote inflating rebate amounts (G1).
//! - `KATPOOL_KRC20_COMMIT_AMOUNT_SOMPI`   default 0.2 KAS (commit P2SH lock)
//! - commit/reveal network fees are sized adaptively from the node fee-rate
//!   (floored at the relay minimum) and frozen per-transfer; not configurable
//! - `KATPOOL_KRC20_BATCH_LIMIT`           default 1000 recipients/tick
//! - `KATPOOL_KRC20_TICKER`                default `NACHO`
//! - `KATPOOL_KRC20_QUOTE_BASE`            default `https://api.coingecko.com`
//!   (KAS-per-NACHO is derived from `CoinGecko` USD spot for both assets)
//! - `KATPOOL_KRC20_QUOTE_BREAKER_THRESHOLD` default 3 consecutive failures
//! - `KATPOOL_KRC20_QUOTE_BREAKER_COOLDOWN_SECS` default 60
//!
//! Treasury UTXO consolidation engine (opt-in; shares the treasury key and the
//! `treasury:spend-leader` advisory lock with the payout engines):
//! - `KATPOOL_CONSOLIDATION_ENABLED`          default `false`
//! - `KATPOOL_CONSOLIDATION_DRY_RUN`          default `true` (plan + sign, no
//!   broadcast). Requires `ENABLED=true` AND `DRY_RUN=false` to move funds.
//! - `KATPOOL_CONSOLIDATION_POLL_SECS`        default 120
//! - `KATPOOL_CONSOLIDATION_TICK_TIMEOUT_SECS` default 120 (per-tick wall-clock
//!   budget; a tick that exceeds it is abandoned and the treasury lock released
//!   so a hung kaspad RPC cannot wedge the payout engines)
//! - `KATPOOL_CONSOLIDATION_TRIGGER_UTXO_COUNT` default 1000 (high-water mark: a
//!   sweep starts only once the spendable UTXO count rises above this)
//! - `KATPOOL_CONSOLIDATION_TARGET_UTXO_COUNT` default 50 (low-water mark: an
//!   active sweep compounds down to this floor, then rests until the count
//!   climbs back above the trigger — hysteresis. Keeping the floor above 1
//!   lets a continuously-mining treasury settle and idle instead of churning a
//!   tiny sweep every tick as fresh coinbase matures in)
//! - `KATPOOL_CONSOLIDATION_MAX_INPUTS_PER_TX` default 80 (upper bound; the
//!   per-transaction mempool standard-mass check is the real input guard, which
//!   caps a one-output self-send near ~88 inputs)
//! - `KATPOOL_CONSOLIDATION_MAX_TXS_PER_TICK` default 50 (sweep throughput;
//!   each tick retires up to this many disjoint batches)

#![cfg_attr(not(test), warn(missing_docs))]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use api::{ApiConfig, AppState, ReadinessHandle};
use kaspa_addresses::{Address, Prefix};
use kaspa_grpc_client::GrpcClient;
use kaspa_rpc_core::notify::mode::NotificationMode;
use kaspa_stratum_bridge::{
    KaspaApi, StratumServerBridgeConfig as BridgeServerConfig, listen_and_serve_with_events, prom,
};
use katpool_db::{PoolConfig, build_pool};
use katpool_domain::{PoolEvent, redact};
use katpool_secrets::{load_from_path, load_from_systemd_credential};
use katpool_telemetry::TelemetryConfig;
use payout_kas::{
    ConsolidationEngine, ConsolidationEngineConfig, DEFAULT_KAS_PAYOUT_THRESHOLD_SOMPI,
    ExecutionMode, GrpcKaspadClient, PayoutEngine, PayoutEngineConfig,
    TREASURY_SPEND_LOCK_NAMESPACE, TickOutcome,
};
use payout_krc20::{
    BreakeredSource, CircuitBreaker, CoinGeckoFloorPrice, DEFAULT_COMMIT_AMOUNT_SOMPI,
    DEFAULT_CYCLE_LIMIT, DEFAULT_HTTP_TIMEOUT, DEFAULT_MIN_NACHO_BASE_UNITS,
    DEFAULT_MIN_PENDING_SOMPI, DEFAULT_QUOTE_BASE, DEFAULT_QUOTE_TICKER, Krc20PayoutEngine,
    Krc20PayoutEngineConfig,
};
use tokio::io::AsyncWriteExt;
use tokio::signal;
use tokio::sync::{broadcast, watch};
use tracing::{error, info, warn};

use accountant::{
    AllocationEngine, ConsumerConfig, EventConsumer, FeeConfig, GeoIp, KaspadGrpcClient,
    KasplexConfig, KasplexTierClassifier, MaturityConfig, MaturityTracker,
    ShieldedRewardScanner, StaticTierClassifier, TierClassifier,
};

// The runtime orchestrator is intentionally long-form: every step
// is a single named operation against the workspace's subsystems,
// and abstracting them out reduces traceability for a path that
// composes multiple critical lifecycles.
#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> Result<()> {
    // Install telemetry before anything else so even config-load failures are
    // captured. `service.name` mirrors the instance id (read directly here so
    // the subscriber is live before the full `RuntimeConfig` parse); format and
    // OTLP export are env-driven (`KATPOOL_LOG_FORMAT`, `KATPOOL_OTLP_ENDPOINT`).
    let telemetry_service =
        std::env::var("KATPOOL_INSTANCE_ID").unwrap_or_else(|_| "katpool-runtime".to_owned());
    let _telemetry = katpool_telemetry::init(&TelemetryConfig::from_env(telemetry_service))
        .context("initializing telemetry")?;

    let arg_list: Vec<String> = std::env::args().skip(1).collect();
    let command = parse_args(&arg_list).context("parsing arguments")?;
    if command == Command::Help {
        print_usage();
        return Ok(());
    }

    let cfg = RuntimeConfig::from_env().context("loading runtime config")?;

    // Operator subcommands run synchronously and exit, never starting the
    // long-running subsystems. Anything else falls through to the daemon.
    match command {
        Command::PayoutRunNow { dry_run } => return run_payout_now(&cfg, dry_run).await,
        Command::TreasuryAudit => return run_treasury_audit(&cfg),
        Command::Daemon | Command::Help => {}
    }

    info!(
        instance = %cfg.instance_id,
        kaspad = %cfg.kaspad_url,
        stratum_port = %cfg.stratum_port,
        network = %cfg.network,
        pool_addresses = ?cfg
            .pool_addresses
            .iter()
            .map(|a| redact::address(&a.to_string()))
            .collect::<Vec<_>>(),
        "katpool runtime starting"
    );

    // ---- DB pool -----------------------------------------------------
    let db = build_pool(&PoolConfig {
        url: cfg.database_url.clone(),
        min_connections: 2,
        max_connections: 16,
        application_name: format!("katpool[{}]", cfg.instance_id),
        ..PoolConfig::production("placeholder".to_owned())
    })
    .await
    .context("opening Postgres pool")?;

    // ---- shared event bus -------------------------------------------
    // Capacity sized for ~3 minutes of sustained 20 shares/s
    // (default 4096); operator-tunable for higher-throughput runs.
    let (event_tx, _event_rx_template) = broadcast::channel::<PoolEvent>(cfg.broadcast_capacity);

    if let Some(record_path) = &cfg.event_record_path {
        info!(path = %record_path, "PoolEvent NDJSON recorder enabled");
        spawn_event_recorder(event_tx.subscribe(), record_path.clone());
    }

    // ---- kaspad clients (bridge + accountant share the URL,
    //      separate connections) ---------------------------------------
    // Custodial PROP-pool mode: every block template the bridge
    // requests from kaspad pays the pool's address (regardless of
    // which miner authorized). The miner-supplied wallet on
    // `mining.authorize` becomes purely the share-credit identity;
    // the accountant pro-rates the matured coinbase across miners
    // by share weight; the payout engine (Phase 4) sends KAS to
    // each miner's authorized address.
    let coinbase_override = cfg
        .pool_addresses
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("KATPOOL_POOL_ADDRESS is empty"))?;
    if cfg.pool_addresses.len() > 1 {
        let coinbase_tag = redact::address(&coinbase_override.to_string());
        warn!(
            "multiple pool addresses supplied; bridge coinbase override uses the first ({coinbase_tag}); accountant reward extraction matches against all"
        );
    }
    // Shutdown channel, created early so the bridge's `KaspaApi` can abort its
    // connection-retry and background stats threads on shutdown (upstream v2.0.0
    // contract). Consumed by the subsystems and the signal task below.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let kaspa_api = KaspaApi::new(
        cfg.kaspad_url.clone(),
        None,
        shutdown_rx.clone(),
        Some(coinbase_override.clone()),
    )
    .await
    .map_err(|e| anyhow::anyhow!("KaspaApi: {e}"))?;
    let tracker_grpc = GrpcClient::connect_with_args(
        NotificationMode::Direct,
        cfg.kaspad_url.clone(),
        None,
        true,
        None,
        false,
        Some(500_000),
        Arc::default(),
    )
    .await
    .context("tracker gRPC connect")?;
    // Reward discovery is address-type driven: a shielded (Orchard) pool
    // address means a ZKas chain, whose coinbase is a shielded note — the
    // transparent UTXO-index client would find nothing, ever. The scanner
    // walks `GetShieldedBlocks` for public coinbase mints to the treasury
    // instead (see `accountant::shielded_scan`). Transparent addresses keep
    // the upstream UTXO path.
    let tracker_grpc = Arc::new(tracker_grpc);
    let all_shielded = cfg
        .pool_addresses
        .iter()
        .all(|a| a.version == kaspa_addresses::Version::ShieldedOrchard);
    let tracker_kaspad: Arc<dyn accountant::KaspadClient> = if all_shielded {
        info!("reward discovery: shielded coinbase scanner (Orchard treasury)");
        Arc::new(
            ShieldedRewardScanner::new(
                Arc::clone(&tracker_grpc),
                db.clone(),
                &cfg.pool_addresses,
                cfg.maturity.coinbase_maturity,
            )
            .context("building shielded reward scanner")?,
        )
    } else {
        info!("reward discovery: transparent coinbase UTXO index");
        Arc::new(KaspadGrpcClient::new(
            tracker_grpc,
            cfg.pool_addresses.clone(),
        ))
    };

    // ---- accountant pipeline ----------------------------------------
    let fee =
        FeeConfig::new(cfg.fee_topline_bps).map_err(|e| anyhow::anyhow!("fee config: {e}"))?;
    let classifier: Arc<dyn TierClassifier> = match cfg.tier_classifier {
        TierClassifierKind::Static => {
            info!(
                "tier classifier: static (every wallet Standard) — the NACHO Elite rebate is \
                 inert; set KATPOOL_TIER_CLASSIFIER=kasplex to enable on-chain tier lookup"
            );
            Arc::new(StaticTierClassifier::standard())
        }
        TierClassifierKind::Kasplex => {
            if cfg.network != "mainnet" {
                warn!(
                    network = %cfg.network,
                    "tier classifier: kasplex selected on a non-mainnet network; the kasplex \
                     indexers are mainnet-only, so testnet wallets resolve as Standard"
                );
            }
            let classifier = KasplexTierClassifier::new(KasplexConfig::default())
                .map_err(|e| anyhow::anyhow!("building kasplex tier classifier: {e}"))?;
            info!(
                "tier classifier: kasplex (on-chain NACHO holdings; safe Standard fallback + \
                 upstream circuit breaker)"
            );
            Arc::new(classifier)
        }
    };
    let engine = Arc::new(AllocationEngine::new(
        db.clone(),
        fee,
        classifier,
        cfg.instance_id.clone(),
    ));
    let tracker = MaturityTracker::new(
        db.clone(),
        tracker_kaspad,
        Arc::clone(&engine),
        cfg.maturity,
        cfg.instance_id.clone(),
    );

    // ---- read-only API + health probes (Phase 6, opt-in) -----------
    // Env-gated like the prom exporter (ADR-0021 A1). Two independent surfaces
    // share one readiness source: the public API (`KATPOOL_API_PORT`) serves
    // `/health` `/ready` `/started` plus `/api/v1`; the dedicated health port
    // (`KATPOOL_HEALTH_CHECK_PORT`) serves only the probes, so orchestrators
    // can health-check even when the public API is off. Readiness reuses work
    // the runtime already does: DB reachability from a periodic `SELECT 1`, and
    // kaspad-sync mirrored from the maturity tracker's existing poll via a
    // `watch` channel — so no second gRPC connection is opened. The plumbing is
    // set up once and shared by both surfaces; the kaspad-sync observer attaches
    // to the tracker (shadowed below) only when at least one surface is enabled.
    let tracker = if cfg.api_bind.is_some() || cfg.health_bind.is_some() {
        // Least-privilege read-only pool for the public API (ADR-0021). When
        // KATPOOL_API_DATABASE_URL is set the API connects as a read-only role,
        // isolated from the writers' full-privilege pool; otherwise it shares
        // `db` (dev / single-role deployments). The readiness probe uses the same
        // pool so /ready reflects the path the API actually serves on.
        let api_db = match cfg.api_database_url.as_deref() {
            Some(url) if !url.is_empty() => build_pool(&PoolConfig {
                url: url.to_owned(),
                min_connections: 2,
                max_connections: 16,
                application_name: format!("katpool-api[{}]", cfg.instance_id),
                ..PoolConfig::production("placeholder".to_owned())
            })
            .await
            .context("opening read-only Postgres pool for the API")?,
            _ => db.clone(),
        };
        let readiness = ReadinessHandle::new();
        let (sync_tx, sync_rx) = watch::channel(false);
        api::spawn_db_readiness_probe(api_db.clone(), readiness.clone());
        spawn_readiness_bridge(sync_rx, readiness.clone());
        let state = AppState::new(api_db, readiness, cfg.api_config.clone());

        if let Some(api_addr) = cfg.api_bind {
            let state = state.clone();
            tokio::spawn(async move {
                if let Err(e) = api::serve_on(api_addr, state).await {
                    error!(error = %e, "public API server exited with error");
                }
            });
            info!(addr = %api_addr, "public read-only API enabled");
        } else {
            info!("public read-only API disabled (set KATPOOL_API_PORT to enable)");
        }

        if let Some(health_addr) = cfg.health_bind {
            tokio::spawn(async move {
                if let Err(e) = api::serve_health_on(health_addr, state).await {
                    error!(error = %e, "health-check endpoint exited with error");
                }
            });
            info!(addr = %health_addr, "health-check endpoint enabled");
        }

        tracker.with_sync_observer(sync_tx)
    } else {
        info!("public read-only API disabled (set KATPOOL_API_PORT to enable)");
        info!("health-check endpoint disabled (set KATPOOL_HEALTH_CHECK_PORT to enable)");
        tracker
    };

    // Optional IP→country resolver (ADR-0025); non-fatal if absent.
    let geoip = load_geoip(cfg.geoip_db_path.as_deref());

    let consumer = EventConsumer::new(
        db.clone(),
        ConsumerConfig::new(cfg.instance_id.clone(), cfg.network.clone())
            .context("building accountant ConsumerConfig")?,
    )
    .with_geoip(geoip);

    // ---- shutdown channel ------------------------------------------
    // (created earlier, before KaspaApi::new, so the bridge node client shares it)
    let signal_task = {
        let tx = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::select! {
                res = signal::ctrl_c() => {
                    if res.is_ok() { info!("SIGINT received"); }
                }
                () = sigterm() => info!("SIGTERM received"),
            }
            if tx.send(true).is_err() {
                warn!("shutdown channel closed before signal arrived");
            }
        })
    };

    // ---- spawn the three subsystems ---------------------------------
    // The consumer takes the shutdown channel so it can DRAIN the broadcast
    // backlog at SIGTERM rather than being aborted mid-buffer (A2). The runtime
    // stops the producer (bridge listener) first in teardown, so every event
    // already on the bus is persisted before exit.
    let event_rx = event_tx.subscribe();
    let consumer_handle = tokio::spawn({
        let consumer = consumer;
        let rx = shutdown_rx.clone();
        let drain_idle = cfg.shutdown_drain_idle;
        let drain_budget = cfg.shutdown_drain_budget;
        async move {
            consumer
                .run_with_shutdown(event_rx, rx, drain_idle, drain_budget)
                .await
        }
    });
    let tracker_handle = tokio::spawn({
        let rx = shutdown_rx.clone();
        async move { tracker.run_loop(rx).await }
    });

    // ---- KAS payout engine (M4.7, opt-in) ---------------------------
    // Single-leader periodic loop: a Postgres advisory lock elects one
    // instance per tick, so running multiple `katpool` replicas is safe.
    // Disabled and dry-run by default — moving funds requires both
    // `KATPOOL_PAYOUT_ENABLED=true` and `KATPOOL_PAYOUT_DRY_RUN=false`.
    let payout_handle = if cfg.payout_enabled {
        let secret = match &cfg.payout.key_source {
            KeySource::File(path) => load_from_path(path)
                .with_context(|| format!("loading treasury key from {}", path.display()))?,
            KeySource::SystemdCredential(name) => load_from_systemd_credential(name)
                .with_context(|| format!("loading treasury credential `{name}`"))?,
        };
        let payout_client = GrpcKaspadClient::connect(cfg.kaspad_url.clone())
            .await
            .context("payout-kas kaspad gRPC connect")?;
        let mode = if cfg.payout.dry_run {
            ExecutionMode::DryRun
        } else {
            ExecutionMode::Live
        };
        let engine = PayoutEngine::new(
            db.clone(),
            payout_client,
            secret,
            coinbase_override.clone(),
            PayoutEngineConfig {
                instance_id: cfg.instance_id.clone(),
                poll_interval: cfg.payout.poll_interval,
                cycle_span_daa: cfg.payout.cycle_span_daa,
                threshold_sompi: cfg.payout.threshold_sompi,
                max_payout_sompi_per_cycle: cfg.payout.max_sompi_per_cycle,
                mode,
                lock_namespace: TREASURY_SPEND_LOCK_NAMESPACE.to_owned(),
            },
        )
        .context("building payout engine")?;
        info!(
            dry_run = cfg.payout.dry_run,
            poll_secs = cfg.payout.poll_interval.as_secs(),
            cycle_span_daa = cfg.payout.cycle_span_daa,
            max_sompi_per_cycle = ?cfg.payout.max_sompi_per_cycle,
            treasury = %redact::address(&coinbase_override.to_string()),
            "payout-kas engine enabled"
        );
        let rx = shutdown_rx.clone();
        Some(tokio::spawn(async move { engine.run_loop(rx).await }))
    } else {
        info!("payout-kas engine disabled (set KATPOOL_PAYOUT_ENABLED=true to enable)");
        None
    };

    // ---- KRC-20 NACHO payout engine (M5.5b, opt-in) -----------------
    // Same single-leader discipline as the KAS engine and the *shared*
    // treasury-spend lock, so the two serialize. Because both tick on the same
    // poll_interval from startup, this engine phase-staggers and waits a bounded
    // interval for the lock (lock_acquire_wait) so it defers to an in-flight KAS
    // payout instead of starving. Shares the treasury key/address and kaspad
    // node (separate gRPC connection). Disabled and dry-run by default.
    let krc20_payout_handle = if cfg.krc20_payout_enabled {
        let secret = match &cfg.krc20_payout.key_source {
            KeySource::File(path) => load_from_path(path)
                .with_context(|| format!("loading treasury key from {}", path.display()))?,
            KeySource::SystemdCredential(name) => load_from_systemd_credential(name)
                .with_context(|| format!("loading treasury credential `{name}`"))?,
        };
        let krc20_client = GrpcKaspadClient::connect(cfg.kaspad_url.clone())
            .await
            .context("payout-krc20 kaspad gRPC connect")?;
        let mode = if cfg.krc20_payout.dry_run {
            ExecutionMode::DryRun
        } else {
            ExecutionMode::Live
        };
        let quote = BreakeredSource::new(
            CoinGeckoFloorPrice::new(cfg.krc20_payout.quote_base.clone(), DEFAULT_HTTP_TIMEOUT)
                .context("building NACHO floor-price client")?,
            CircuitBreaker::new(
                cfg.krc20_payout.breaker_threshold,
                cfg.krc20_payout.breaker_cooldown,
            ),
        );
        let engine = Krc20PayoutEngine::new(
            db.clone(),
            krc20_client,
            secret,
            coinbase_override.clone(),
            quote,
            Krc20PayoutEngineConfig {
                instance_id: cfg.instance_id.clone(),
                poll_interval: cfg.krc20_payout.poll_interval,
                // Bounded wait for the shared treasury lock: a quarter of the
                // poll interval comfortably outlasts an in-flight KAS payout
                // tick (so this engine merely defers to it) yet stays well under
                // one period. Phase-staggering already keeps real contention
                // rare; this is the safety net.
                lock_acquire_wait: cfg.krc20_payout.poll_interval / 4,
                cycle_span_daa: cfg.krc20_payout.cycle_span_daa,
                mode,
                lock_namespace: TREASURY_SPEND_LOCK_NAMESPACE.to_owned(),
                min_pending_sompi: cfg.krc20_payout.min_pending_sompi,
                min_nacho_base_units: cfg.krc20_payout.min_nacho_base_units,
                ticker: cfg.krc20_payout.ticker.clone(),
                commit_amount_sompi: cfg.krc20_payout.commit_amount_sompi,
                batch_limit: cfg.krc20_payout.batch_limit,
                max_nacho_base_units_per_cycle: cfg.krc20_payout.max_nacho_base_units_per_cycle,
            },
        )
        .context("building krc20 payout engine")?;
        info!(
            dry_run = cfg.krc20_payout.dry_run,
            poll_secs = cfg.krc20_payout.poll_interval.as_secs(),
            cycle_span_daa = cfg.krc20_payout.cycle_span_daa,
            ticker = %cfg.krc20_payout.ticker,
            max_nacho_per_cycle = ?cfg.krc20_payout.max_nacho_base_units_per_cycle,
            treasury = %redact::address(&coinbase_override.to_string()),
            "payout-krc20 engine enabled"
        );
        let rx = shutdown_rx.clone();
        Some(tokio::spawn(async move { engine.run_loop(rx).await }))
    } else {
        info!("payout-krc20 engine disabled (set KATPOOL_KRC20_PAYOUT_ENABLED=true to enable)");
        None
    };

    // ---- ZKas shielded payout engine (opt-in) ------------------------
    // The payout path that actually applies to ZKas (shielded treasury,
    // one Orchard tx per recipient via the `shielded-pay` CLI). Shares the
    // `treasury:spend-leader` advisory lock with the engines above, so at
    // most one treasury spender acts per tick. Disabled and dry-run by
    // default — moving funds requires both KATPOOL_ZKAS_PAYOUT_ENABLED=true
    // and KATPOOL_ZKAS_PAYOUT_DRY_RUN=false, plus the shielded-pay binary
    // path and the treasury seed file. Env-configured (not katpool-config)
    // while the engine is new; promote once the knobs settle.
    let zkas_payout_enabled = std::env::var("KATPOOL_ZKAS_PAYOUT_ENABLED")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let zkas_payout_handle = if zkas_payout_enabled {
        let env_u64 = |key: &str, default: u64| -> Result<u64> {
            match std::env::var(key) {
                Ok(v) => v.parse::<u64>().with_context(|| format!("{key}={v}: not a u64")),
                Err(_) => Ok(default),
            }
        };
        let bin = std::env::var("KATPOOL_SHIELDED_PAY_BIN")
            .context("KATPOOL_ZKAS_PAYOUT_ENABLED=true requires KATPOOL_SHIELDED_PAY_BIN")?;
        let seed_file = std::env::var("KATPOOL_ZKAS_TREASURY_SEED_FILE")
            .context("KATPOOL_ZKAS_PAYOUT_ENABLED=true requires KATPOOL_ZKAS_TREASURY_SEED_FILE")?;
        let dry_run = std::env::var("KATPOOL_ZKAS_PAYOUT_DRY_RUN")
            .map(|v| !(v.eq_ignore_ascii_case("false") || v == "0"))
            .unwrap_or(true);
        let poll_secs = env_u64("KATPOOL_ZKAS_POLL_SECS", 600)?;
        let cycle_span_daa = env_u64("KATPOOL_ZKAS_CYCLE_SPAN_DAA", 3_600)?;
        let threshold_sompi = i64::try_from(env_u64(
            "KATPOOL_ZKAS_THRESHOLD_SOMPI",
            katpool_db::repo::payout::DEFAULT_ZKAS_PAYOUT_THRESHOLD_SOMPI as u64,
        )?)
        .context("KATPOOL_ZKAS_THRESHOLD_SOMPI out of range")?;
        let spend_cap_sompi = match std::env::var("KATPOOL_ZKAS_SPEND_CAP_SOMPI") {
            Ok(v) => Some(v.parse::<i64>().with_context(|| format!("KATPOOL_ZKAS_SPEND_CAP_SOMPI={v}"))?),
            Err(_) => None,
        };
        let max_sends_per_tick = usize::try_from(env_u64("KATPOOL_ZKAS_MAX_SENDS_PER_TICK", 3)?)
            .context("KATPOOL_ZKAS_MAX_SENDS_PER_TICK out of range")?;
        let per_wallet_cap_sompi = i64::try_from(env_u64(
            "KATPOOL_ZKAS_MAX_PER_WALLET_SOMPI",
            payout_zkas::DEFAULT_PER_WALLET_CAP_SOMPI as u64,
        )?)
        .context("KATPOOL_ZKAS_MAX_PER_WALLET_SOMPI out of range")?;
        let fee_sompi = env_u64("KATPOOL_ZKAS_PAYOUT_FEE_SOMPI", payout_zkas::DEFAULT_PAYOUT_FEE_SOMPI)?;
        let anchor_depth = match std::env::var("KATPOOL_ZKAS_ANCHOR_DEPTH") {
            Ok(v) => Some(v.parse::<u64>().with_context(|| format!("KATPOOL_ZKAS_ANCHOR_DEPTH={v}"))?),
            Err(_) => None,
        };

        let zkas_grpc = GrpcClient::connect_with_args(
            NotificationMode::Direct,
            cfg.kaspad_url.clone(),
            None,
            true,
            None,
            false,
            Some(500_000),
            Arc::default(),
        )
        .await
        .context("zkas payout gRPC connect")?;

        let sender = payout_zkas::ShieldedPayCli {
            bin: bin.into(),
            rpc_server: cfg.kaspad_url.trim_start_matches("grpc://").to_owned(),
            seed_file: seed_file.into(),
            fee_sompi,
            anchor_depth,
            timeout: payout_zkas::DEFAULT_SEND_TIMEOUT,
        };
        let mode = if dry_run {
            payout_zkas::ExecutionMode::DryRun
        } else {
            payout_zkas::ExecutionMode::Live
        };
        let engine = payout_zkas::ZkasPayoutEngine::new(
            db.clone(),
            Box::new(sender),
            Box::new(payout_zkas::GrpcChainReader::new(Arc::new(zkas_grpc))),
            payout_zkas::ZkasPayoutEngineConfig {
                tick_interval: Duration::from_secs(poll_secs),
                cycle_span_daa,
                threshold_sompi,
                per_wallet_cap_sompi,
                spend_cap_sompi,
                mode,
                max_sends_per_tick,
                lock_namespace: TREASURY_SPEND_LOCK_NAMESPACE.to_owned(),
                instance_id: cfg.instance_id.clone(),
            },
        )
        .context("building zkas payout engine")?;
        info!(
            dry_run,
            poll_secs,
            cycle_span_daa,
            threshold_sompi,
            spend_cap_sompi = ?spend_cap_sompi,
            max_sends_per_tick,
            "payout-zkas engine enabled"
        );
        let rx = shutdown_rx.clone();
        Some(tokio::spawn(async move { engine.run_loop(rx).await }))
    } else {
        info!("payout-zkas engine disabled (set KATPOOL_ZKAS_PAYOUT_ENABLED=true to enable)");
        None
    };

    // ---- Treasury UTXO consolidation engine (opt-in) ----------------
    // Fourth treasury task. Shares the single `treasury:spend-leader`
    // advisory lock with both payout engines, so only one treasury spender
    // acts per tick and they can never select the same UTXO. Disabled and
    // dry-run by default.
    let consolidation_handle = if cfg.consolidation_enabled {
        let secret = match &cfg.consolidation.key_source {
            KeySource::File(path) => load_from_path(path)
                .with_context(|| format!("loading treasury key from {}", path.display()))?,
            KeySource::SystemdCredential(name) => load_from_systemd_credential(name)
                .with_context(|| format!("loading treasury credential `{name}`"))?,
        };
        let consolidation_client = GrpcKaspadClient::connect(cfg.kaspad_url.clone())
            .await
            .context("consolidation kaspad gRPC connect")?;
        let mode = if cfg.consolidation.dry_run {
            ExecutionMode::DryRun
        } else {
            ExecutionMode::Live
        };
        let engine = ConsolidationEngine::new(
            db.clone(),
            consolidation_client,
            secret,
            coinbase_override.clone(),
            ConsolidationEngineConfig {
                instance_id: cfg.instance_id.clone(),
                poll_interval: cfg.consolidation.poll_interval,
                tick_timeout: cfg.consolidation.tick_timeout,
                // Bounded wait for the shared treasury lock: a quarter of the
                // poll interval is generous enough to outlast an in-flight
                // payout tick (so consolidation merely defers to it) yet stays
                // well under one period. Phase-staggering already keeps real
                // contention rare; this is the safety net.
                lock_acquire_wait: cfg.consolidation.poll_interval / 4,
                mode,
                trigger_utxo_count: cfg.consolidation.trigger_utxo_count,
                target_utxo_count: cfg.consolidation.target_utxo_count,
                max_inputs_per_tx: cfg.consolidation.max_inputs_per_tx,
                max_txs_per_tick: cfg.consolidation.max_txs_per_tick,
                lock_namespace: TREASURY_SPEND_LOCK_NAMESPACE.to_owned(),
            },
        );
        info!(
            dry_run = cfg.consolidation.dry_run,
            poll_secs = cfg.consolidation.poll_interval.as_secs(),
            tick_timeout_secs = cfg.consolidation.tick_timeout.as_secs(),
            trigger_utxo_count = cfg.consolidation.trigger_utxo_count,
            target_utxo_count = cfg.consolidation.target_utxo_count,
            max_inputs_per_tx = cfg.consolidation.max_inputs_per_tx,
            max_txs_per_tick = cfg.consolidation.max_txs_per_tick,
            treasury = %redact::address(&coinbase_override.to_string()),
            "treasury consolidation engine enabled"
        );
        let rx = shutdown_rx.clone();
        Some(tokio::spawn(async move { engine.run_loop(rx).await }))
    } else {
        info!(
            "treasury consolidation engine disabled (set KATPOOL_CONSOLIDATION_ENABLED=true to enable)"
        );
        None
    };

    // Bridge is the long-running stratum listener. Its
    // `listen_and_serve_with_events` doesn't currently respect a
    // shutdown channel (upstream limitation); we drive its lifetime
    // off the JoinHandle here and depend on SIGTERM/SIGINT killing
    // the process when we want it down.
    let bridge_config = BridgeServerConfig {
        instance_id: cfg.instance_id.clone(),
        stratum_port: cfg.stratum_port.clone(),
        stratum_ports: cfg.stratum_ports.clone(),
        kaspad_address: cfg.kaspad_url.clone(),
        prom_port: cfg.prom_port.clone(),
        print_stats: false,
        log_to_file: false,
        health_check_port: cfg.health_check_port.clone(),
        block_wait_time: Duration::from_millis(500),
        min_share_diff: cfg.min_share_diff as f64,
        var_diff: cfg.var_diff,
        shares_per_min: cfg.shares_per_min,
        var_diff_stats: false,
        extranonce_size: 2,
        pow2_clamp: true,
        coinbase_tag_suffix: None,
        proxy_protocol: cfg.proxy_protocol,
    };
    if cfg.stratum_ports.is_empty() {
        info!(port = %cfg.stratum_port, "stratum: single-port mode");
    } else {
        info!(ports = ?cfg.stratum_ports, "stratum: multi-port mode with per-port difficulty seeds");
    }
    let bridge_tx = event_tx.clone();
    let bridge_api = Arc::clone(&kaspa_api);
    let bridge_concrete = Some(Arc::clone(&kaspa_api));
    let bridge_handle = tokio::spawn(async move {
        listen_and_serve_with_events(bridge_config, bridge_api, bridge_concrete, Some(bridge_tx))
            .await
    });

    // Export Prometheus metrics when KATPOOL_PROM_PORT is set. The unified
    // runtime must start this itself — unlike the standalone bridge binary,
    // `listen_and_serve_with_events` does not. `start_prom_server` also runs
    // `init_metrics()`; without it every bridge `record_*` call is a no-op, so
    // this is what activates the anti-abuse counters as well as the exporter.
    if cfg.prom_port.is_empty() {
        info!("prometheus metrics disabled (set KATPOOL_PROM_PORT to enable)");
    } else {
        let prom_port = cfg.prom_port.clone();
        let prom_instance = cfg.instance_id.clone();
        info!(port = %prom_port, "prometheus metrics server enabled");
        // Register payout/treasury metrics (B7) into the same global registry the
        // exporter gathers; the engines' record_* calls are no-ops until this runs.
        katpool_metrics::init_payout_metrics();
        tokio::spawn(async move {
            if let Err(e) = prom::start_prom_server(&prom_port, &prom_instance).await {
                error!("prometheus metrics server error: {e}");
            }
        });
    }

    info!("subsystems running; awaiting shutdown signal");

    // ---- wait for shutdown ------------------------------------------
    let mut shutdown_observer = shutdown_rx;
    let _ = shutdown_observer.changed().await;
    info!("shutdown signal observed; tearing down subsystems");

    // Shutdown semantics by subsystem (Phase 7 wiring rework, A2):
    //
    // - **Bridge** is stopped FIRST so it stops producing events. Its
    //   `listen_and_serve_with_events` has no cooperative shutdown (an upstream
    //   limitation; the detached per-connection + kaspad-notification tasks
    //   survive the listener-task abort and keep cloned `PoolEvent` senders
    //   alive), so aborting the listener handle is still how we stop it.
    // - **Consumer** then DRAINS the broadcast backlog: it observes the same
    //   `shutdown_rx`, stops blocking on new events, and persists everything
    //   already on the bus before returning (bounded by `shutdown_drain_*`).
    //   Because the producer is already stopped, in steady state nothing on the
    //   bus is lost — unlike the previous abort-mid-buffer path. The narrow
    //   residual (events a detached bridge task emits during the drain window)
    //   is bounded by the idle gap and documented until the vendored bridge
    //   grows a cooperative shutdown (ADR-0002 follow-up).
    // - **Tracker** and the **payout engines** honor `shutdown_rx` and exit at
    //   their next tick; we await them cleanly afterwards.
    bridge_handle.abort();
    let _ = bridge_handle.await;
    drop(event_tx);
    match consumer_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => error!(error = %e, "consumer exited with error"),
        Err(e) => error!(error = %e, "consumer task join error"),
    }
    if let Err(e) = tracker_handle.await? {
        error!(error = %e, "tracker exited with error");
    }
    // The payout engines honor the shutdown channel; await them cleanly.
    if let Some(handle) = payout_handle {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(error = %e, "payout engine exited with error"),
            Err(e) => error!(error = %e, "payout engine task join error"),
        }
    }
    if let Some(handle) = krc20_payout_handle {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(error = %e, "krc20 payout engine exited with error"),
            Err(e) => error!(error = %e, "krc20 payout engine task join error"),
        }
    }
    if let Some(handle) = zkas_payout_handle {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(error = %e, "zkas payout engine exited with error"),
            Err(e) => error!(error = %e, "zkas payout engine task join error"),
        }
    }
    if let Some(handle) = consolidation_handle {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(error = %e, "consolidation engine exited with error"),
            Err(e) => error!(error = %e, "consolidation engine task join error"),
        }
    }
    signal_task.abort();
    let _ = signal_task.await;

    info!("katpool runtime exiting cleanly");
    Ok(())
}

/// CLI command selected from process arguments.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    /// Run the full pool runtime (default; no arguments).
    Daemon,
    /// Trigger a single KAS payout cycle synchronously, then exit.
    PayoutRunNow {
        /// Force dry-run (sign + verify only) regardless of `KATPOOL_PAYOUT_DRY_RUN`.
        dry_run: bool,
    },
    /// Audit that the loaded treasury key controls the configured treasury
    /// address (read-only), then exit non-zero on mismatch.
    TreasuryAudit,
    /// Print usage and exit.
    Help,
}

/// Parse process arguments (excluding `argv[0]`) into a [`Command`].
///
/// Kept dependency-free and pure so it is exhaustively unit-testable. The
/// daemon is the default so the systemd unit (which passes no arguments)
/// is unaffected.
fn parse_args(args: &[String]) -> Result<Command> {
    let mut iter = args.iter().map(String::as_str);
    match iter.next() {
        None => Ok(Command::Daemon),
        Some("-h" | "--help" | "help") => Ok(Command::Help),
        Some("payout") => match iter.next() {
            Some("run-now") => {
                let mut dry_run = false;
                for arg in iter {
                    match arg {
                        "--dry-run" => dry_run = true,
                        other => anyhow::bail!("unknown flag for `payout run-now`: {other}"),
                    }
                }
                Ok(Command::PayoutRunNow { dry_run })
            }
            Some(other) => {
                anyhow::bail!("unknown `payout` subcommand: {other} (expected `run-now`)")
            }
            None => anyhow::bail!("`payout` requires a subcommand (e.g. `run-now`)"),
        },
        Some("treasury") => match iter.next() {
            Some("audit") => {
                if let Some(other) = iter.next() {
                    anyhow::bail!("unknown flag for `treasury audit`: {other}");
                }
                Ok(Command::TreasuryAudit)
            }
            Some(other) => {
                anyhow::bail!("unknown `treasury` subcommand: {other} (expected `audit`)")
            }
            None => anyhow::bail!("`treasury` requires a subcommand (e.g. `audit`)"),
        },
        Some(other) => anyhow::bail!("unknown command: {other} (try `--help`)"),
    }
}

// Help text is operator-facing and must reach the terminal regardless of the
// tracing filter, so stdout is correct here (unlike runtime diagnostics).
#[allow(clippy::print_stdout)]
fn print_usage() {
    println!(
        "katpool — Kaspa mining pool runtime\n\n\
         USAGE:\n  \
         katpool                          Run the full pool daemon (default)\n  \
         katpool payout run-now [--dry-run]\n                                   \
         Drive one KAS payout cycle now, then exit\n  \
         katpool treasury audit           Verify the loaded key controls the\n                                   \
         configured treasury address, then exit (non-zero on mismatch)\n  \
         katpool --help                   Show this help\n\n\
         Configuration is environment-variable driven (see the module docs and\n\
         ops/env/<network>.env). `payout run-now` honours the same settings as\n\
         the daemon — including KATPOOL_PAYOUT_DRY_RUN — and coordinates with a\n\
         running daemon through the shared payout leader lock, so only one cycle\n\
         driver acts at a time. Pass `--dry-run` to preview without broadcasting."
    );
}

/// Audit that the loaded treasury key controls the configured treasury address
/// (Phase 8 / Runbook 11). Read-only: derives the key's schnorr P2PK address and
/// compares it to `KATPOOL_POOL_ADDRESS`. Logs a structured audit record and
/// exits non-zero on mismatch, so a systemd timer's `OnFailure=` (or a Loki
/// alert on the structured error log) pages the operator. Never moves funds.
fn run_treasury_audit(cfg: &RuntimeConfig) -> Result<()> {
    let expected = cfg
        .pool_addresses
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("KATPOOL_POOL_ADDRESS is empty"))?;
    let secret = match &cfg.payout.key_source {
        KeySource::File(path) => load_from_path(path)
            .with_context(|| format!("loading treasury key from {}", path.display()))?,
        KeySource::SystemdCredential(name) => load_from_systemd_credential(name)
            .with_context(|| format!("loading treasury credential `{name}`"))?,
    };
    let derived = payout_kas::treasury_address_from_secret(&secret, expected.prefix)
        .context("deriving treasury address from the loaded key")?;
    let expected_tag = redact::address(&expected.to_string());
    if derived == expected {
        info!(
            treasury = %expected_tag,
            result = "ok",
            "treasury key audit: the loaded key controls the configured treasury address"
        );
        Ok(())
    } else {
        error!(
            expected = %expected_tag,
            derived = %redact::address(&derived.to_string()),
            result = "mismatch",
            "TREASURY KEY AUDIT FAILED: loaded key does NOT control the configured treasury address (rotation/compromise/misconfig)"
        );
        anyhow::bail!(
            "treasury key/address mismatch: configured address is not controlled by the loaded key"
        )
    }
}

/// Operator on-demand payout: drive the current DAA-window cycle exactly as a
/// single daemon tick would (plan → broadcast → confirm → reconcile), under the
/// shared `treasury:spend-leader` advisory lock, then exit.
///
/// Safe to invoke while the daemon runs: the advisory lock guarantees only one
/// treasury spender acts at a time. If another spender holds it mid-tick the
/// lock is briefly retried before giving up.
async fn run_payout_now(cfg: &RuntimeConfig, force_dry_run: bool) -> Result<()> {
    let db = build_pool(&PoolConfig {
        url: cfg.database_url.clone(),
        min_connections: 1,
        max_connections: 4,
        application_name: format!("katpool-payout-run-now[{}]", cfg.instance_id),
        ..PoolConfig::production("placeholder".to_owned())
    })
    .await
    .context("opening Postgres pool")?;

    let treasury_address = cfg
        .pool_addresses
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("KATPOOL_POOL_ADDRESS is empty"))?;

    let secret = match &cfg.payout.key_source {
        KeySource::File(path) => load_from_path(path)
            .with_context(|| format!("loading treasury key from {}", path.display()))?,
        KeySource::SystemdCredential(name) => load_from_systemd_credential(name)
            .with_context(|| format!("loading treasury credential `{name}`"))?,
    };

    let client = GrpcKaspadClient::connect(cfg.kaspad_url.clone())
        .await
        .context("payout-kas kaspad gRPC connect")?;

    let mode = if force_dry_run || cfg.payout.dry_run {
        ExecutionMode::DryRun
    } else {
        ExecutionMode::Live
    };

    let engine = PayoutEngine::new(
        db,
        client,
        secret,
        treasury_address.clone(),
        PayoutEngineConfig {
            instance_id: cfg.instance_id.clone(),
            poll_interval: cfg.payout.poll_interval,
            cycle_span_daa: cfg.payout.cycle_span_daa,
            threshold_sompi: cfg.payout.threshold_sompi,
            max_payout_sompi_per_cycle: cfg.payout.max_sompi_per_cycle,
            mode,
            lock_namespace: TREASURY_SPEND_LOCK_NAMESPACE.to_owned(),
        },
    )
    .context("building payout engine")?;

    info!(
        dry_run = mode.is_dry_run(),
        threshold_sompi = cfg.payout.threshold_sompi,
        cycle_span_daa = cfg.payout.cycle_span_daa,
        treasury = %redact::address(&treasury_address.to_string()),
        "payout run-now: driving current cycle"
    );

    // The daemon may hold the leader lock mid-tick; retry briefly before failing.
    let mut attempt = 0_u32;
    let report = loop {
        attempt += 1;
        match engine.run_once().await.context("payout run-now tick")? {
            TickOutcome::Ran(report) => break report,
            TickOutcome::SkippedNotLeader if attempt < 10 => {
                warn!(attempt, "payout leader lock held elsewhere; retrying in 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            TickOutcome::SkippedNotLeader => {
                anyhow::bail!("another instance holds the payout leader lock; try again shortly");
            }
        }
    };

    let broadcast = &report.broadcast;
    info!(
        cycle_id = report.cycle_id,
        status = ?report.status,
        dry_run = mode.is_dry_run(),
        planned_batches = broadcast.planned_batches,
        submitted_payouts = broadcast.submitted_payouts,
        accepted = report.confirm.accepted,
        confirmed = report.confirm.confirmed,
        deferred_below_floor = broadcast.deferred_below_floor,
        unpaid = broadcast.unpaid,
        "payout run-now complete"
    );
    if !broadcast.submit_errors.is_empty() {
        error!(
            errors = broadcast.submit_errors.len(),
            detail = %broadcast.submit_errors.join("; "),
            "payout run-now: broadcast(s) rejected"
        );
        anyhow::bail!(
            "{} payout broadcast(s) were rejected",
            broadcast.submit_errors.len()
        );
    }
    Ok(())
}

async fn sigterm() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut sig) = signal(SignalKind::terminate()) {
            sig.recv().await;
        }
    }
    #[cfg(not(unix))]
    {
        std::future::pending::<()>().await;
    }
}

/// Load the optional `GeoIP` country resolver (ADR-0025).
///
/// A missing or unreadable database is non-fatal: it logs and returns
/// `None` so a `GeoIP` misconfiguration never takes down the pool. `None`
/// path ⇒ geo disabled.
fn load_geoip(path: Option<&str>) -> Option<Arc<GeoIp>> {
    let Some(path) = path else {
        info!("GeoIP disabled (set KATPOOL_GEOIP_DB to enable session geo)");
        return None;
    };
    match GeoIp::open(path) {
        Ok(g) => {
            info!(%path, "GeoIP country resolver loaded (session geo enabled)");
            Some(Arc::new(g))
        }
        Err(e) => {
            warn!(%path, error = %e, "GeoIP database failed to load; session geo disabled");
            None
        }
    }
}

// This is an env-config DTO: each bool maps 1:1 to a documented
// `KATPOOL_*` toggle. Collapsing them into enums would obscure that
// mapping without improving safety, so the bool-count lint is waived here.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct RuntimeConfig {
    kaspad_url: String,
    database_url: String,
    /// Optional least-privilege read-only Postgres URL for the public API
    /// (ADR-0021). Unset ⇒ the API shares the writers' pool.
    api_database_url: Option<String>,
    pool_addresses: Vec<Address>,
    stratum_port: String,
    /// Multi-port stratum binding with per-port starting-difficulty
    /// seeds (ADR-0022), parsed from `KATPOOL_STRATUM_PORTS`. Each entry
    /// is `(port, seed)`; the seed is the *initial* difficulty for
    /// connections on that port and vardiff moves freely from there.
    /// Empty => single-port mode using [`Self::stratum_port`].
    stratum_ports: Vec<(String, u32)>,
    prom_port: String,
    health_check_port: String,
    instance_id: String,
    fee_topline_bps: u16,
    /// Per-miner stratum difficulty floor and (when `var_diff` is off)
    /// pin point. ASIC-class default is 4096; vardiff lifts from here.
    min_share_diff: u32,
    /// Enable the bridge's variable-difficulty retarget loop. When `false`,
    /// every connection is pinned at [`Self::min_share_diff`] for its
    /// lifetime, which on a fast-block-rate network like Kaspa causes
    /// ASIC-class miners to flood low-difficulty shares that go stale
    /// against newer block templates.
    var_diff: bool,
    /// Target accepted-shares-per-minute that the vardiff retarget loop
    /// converges each miner toward; ignored when `var_diff` is `false`.
    shares_per_min: u32,
    /// Require + parse a PROXY protocol v2 header on every stratum
    /// connection to recover the real miner IP behind the fly.io edge
    /// (ADR-0022). Default `false`. Mainnet (fronted by the edge) sets
    /// it `true`; the origin stratum ports must be firewalled to the
    /// forwarder egress when enabled.
    proxy_protocol: bool,
    broadcast_capacity: usize,
    maturity: MaturityConfig,
    /// Network identifier passed to the accountant for
    /// `wallet::ensure`. One of `mainnet`, `testnet-10`,
    /// `testnet-11`, `devnet`, `simnet` (see
    /// [`accountant::consumer::VALID_NETWORKS`]). Derived from the
    /// pool address prefix unless `KATPOOL_NETWORK` overrides it
    /// (testnet-11 must be set explicitly because the bech32 prefix
    /// is shared with testnet-10).
    network: String,
    /// When set, append one serde-json `PoolEvent` per line to this path.
    event_record_path: Option<String>,
    /// Optional GeoLite2/GeoIP2 Country `.mmdb` path (`KATPOOL_GEOIP_DB`,
    /// ADR-0025). When set and loadable, the accountant tags each session
    /// with an ISO-3166 country; unset/absent ⇒ geo disabled (NULL country).
    geoip_db_path: Option<String>,
    /// Bind address for the public read-only API (`KATPOOL_API_PORT`).
    /// `None` disables the API (mirrors the prom exporter's env gate).
    api_bind: Option<SocketAddr>,
    /// Bind address for the dedicated liveness/readiness probe endpoint
    /// (`KATPOOL_HEALTH_CHECK_PORT`). Serves only `/health` `/ready`
    /// `/started`, reusing the same readiness plumbing as the API so health
    /// is observable with or without `api_bind`. `None` disables it.
    health_bind: Option<SocketAddr>,
    /// API knobs (rate limit, cache TTLs, timeout, CORS). Parsed
    /// unconditionally; only consumed when `api_bind` is `Some`.
    api_config: ApiConfig,
    /// Whether the KAS payout engine runs in this process.
    payout_enabled: bool,
    /// Payout engine knobs (parsed unconditionally; only consumed when
    /// `payout_enabled`).
    payout: PayoutConfig,
    /// Whether the KRC-20 NACHO payout engine runs in this process.
    krc20_payout_enabled: bool,
    /// KRC-20 engine knobs (parsed unconditionally; only consumed when
    /// `krc20_payout_enabled`).
    krc20_payout: Krc20RuntimeConfig,
    /// Whether the treasury UTXO consolidation engine runs in this process.
    consolidation_enabled: bool,
    /// Consolidation engine knobs (parsed unconditionally; only consumed when
    /// `consolidation_enabled`).
    consolidation: ConsolidationConfig,
    /// Which wallet-tier classifier the accountant uses (ADR-0012).
    tier_classifier: TierClassifierKind,
    /// Idle gap at shutdown after which the event backlog is considered drained.
    shutdown_drain_idle: Duration,
    /// Hard ceiling on the shutdown backlog drain (defence against a producer
    /// that never goes idle).
    shutdown_drain_budget: Duration,
}

/// Default idle gap that signals the shutdown backlog is drained.
const DEFAULT_SHUTDOWN_DRAIN_IDLE: Duration = Duration::from_millis(500);

/// Default hard ceiling on the shutdown backlog drain.
const DEFAULT_SHUTDOWN_DRAIN_BUDGET_SECS: u64 = 10;

/// Which wallet-tier classifier the accountant uses (`KATPOOL_TIER_CLASSIFIER`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TierClassifierKind {
    /// Every wallet is `Standard` (the NACHO Elite rebate is inert). Default.
    Static,
    /// On-chain NACHO holdings via the kasplex indexers (mainnet-oriented),
    /// with a safe `Standard` fallback and an upstream circuit breaker.
    Kasplex,
}

impl TierClassifierKind {
    /// Parse from an already-resolved raw value (env-or-file). `None` ⇒
    /// [`Self::Static`].
    fn parse(raw: Option<&str>) -> Result<Self> {
        match raw {
            None | Some("static") => Ok(Self::Static),
            Some("kasplex") => Ok(Self::Kasplex),
            Some(other) => {
                anyhow::bail!("KATPOOL_TIER_CLASSIFIER=`{other}`: expected `static` or `kasplex`")
            }
        }
    }
}

/// Where the treasury signing key is loaded from at startup.
#[derive(Debug, Clone)]
enum KeySource {
    /// systemd `LoadCredentialEncrypted` credential name (production).
    SystemdCredential(String),
    /// Raw 32-byte hex key file (testnet rehearsal / M4.8).
    File(PathBuf),
}

/// Parsed KAS payout engine configuration.
#[derive(Debug)]
struct PayoutConfig {
    dry_run: bool,
    poll_interval: Duration,
    cycle_span_daa: u64,
    threshold_sompi: i64,
    /// Optional per-cycle KAS spend cap (sompi); `None` disables it (G1).
    max_sompi_per_cycle: Option<i64>,
    key_source: KeySource,
}

/// Parsed treasury UTXO consolidation engine configuration.
#[derive(Debug)]
struct ConsolidationConfig {
    dry_run: bool,
    poll_interval: Duration,
    tick_timeout: Duration,
    trigger_utxo_count: usize,
    target_utxo_count: usize,
    max_inputs_per_tx: usize,
    max_txs_per_tick: usize,
    key_source: KeySource,
}

/// Parsed KRC-20 NACHO payout engine configuration.
#[derive(Debug)]
struct Krc20RuntimeConfig {
    dry_run: bool,
    poll_interval: Duration,
    cycle_span_daa: u64,
    min_pending_sompi: i64,
    min_nacho_base_units: u128,
    commit_amount_sompi: u64,
    batch_limit: i64,
    /// Optional per-cycle NACHO spend cap (base units); `None` disables it (G1).
    max_nacho_base_units_per_cycle: Option<i64>,
    ticker: String,
    quote_base: String,
    breaker_threshold: u32,
    breaker_cooldown: Duration,
    key_source: KeySource,
}

impl Krc20RuntimeConfig {
    fn from_env(key_source: KeySource) -> Result<Self> {
        Ok(Self {
            dry_run: optional_bool("KATPOOL_KRC20_PAYOUT_DRY_RUN")?.unwrap_or(true),
            poll_interval: Duration::from_secs(
                optional_u64("KATPOOL_KRC20_PAYOUT_POLL_SECS")?.unwrap_or(60),
            ),
            cycle_span_daa: optional_u64("KATPOOL_KRC20_PAYOUT_CYCLE_SPAN_DAA")?.unwrap_or(216_000),
            min_pending_sompi: optional_i64("KATPOOL_KRC20_MIN_PENDING_SOMPI")?
                .unwrap_or(DEFAULT_MIN_PENDING_SOMPI),
            min_nacho_base_units: optional_u128("KATPOOL_KRC20_MIN_NACHO_BASE_UNITS")?
                .unwrap_or(DEFAULT_MIN_NACHO_BASE_UNITS),
            commit_amount_sompi: optional_u64("KATPOOL_KRC20_COMMIT_AMOUNT_SOMPI")?
                .unwrap_or(DEFAULT_COMMIT_AMOUNT_SOMPI),
            batch_limit: optional_i64("KATPOOL_KRC20_BATCH_LIMIT")?.unwrap_or(DEFAULT_CYCLE_LIMIT),
            max_nacho_base_units_per_cycle: optional_i64("KATPOOL_KRC20_MAX_NACHO_PER_CYCLE")?,
            ticker: optional("KATPOOL_KRC20_TICKER")
                .unwrap_or_else(|| DEFAULT_QUOTE_TICKER.to_owned()),
            quote_base: optional("KATPOOL_KRC20_QUOTE_BASE")
                .unwrap_or_else(|| DEFAULT_QUOTE_BASE.to_owned()),
            breaker_threshold: optional_u32("KATPOOL_KRC20_QUOTE_BREAKER_THRESHOLD")?.unwrap_or(3),
            breaker_cooldown: Duration::from_secs(
                optional_u64("KATPOOL_KRC20_QUOTE_BREAKER_COOLDOWN_SECS")?.unwrap_or(60),
            ),
            key_source,
        })
    }
}

impl RuntimeConfig {
    // One flat env-var read per field across many subsystems; keeping it linear
    // is clearer than splitting into per-subsystem helpers that each run once.
    #[allow(clippy::too_many_lines)]
    fn from_env() -> Result<Self> {
        // Optional file layer (A3). `KATPOOL_CONFIG` points at a YAML/TOML
        // file parsed + validated by `katpool-config`. Precedence is strict:
        // environment variable > config file > built-in default — so env
        // always wins and the file only fills gaps. Unset/empty `KATPOOL_CONFIG`
        // ⇒ an empty file layer ⇒ behavior identical to pure-env config.
        let file = match optional("KATPOOL_CONFIG") {
            Some(path) => katpool_config::FileConfig::load(std::path::Path::new(&path))
                .with_context(|| format!("loading KATPOOL_CONFIG=`{path}`"))?,
            None => katpool_config::FileConfig::default(),
        };

        let kaspad_url = require("KASPAD_GRPC_URL", file.kaspad_url.clone())?;
        let database_url = require("KATPOOL_DATABASE_URL", file.database_url.clone())?;
        // Optional read-only URL for the public API (least privilege, ADR-0021).
        // Env-only (a connection secret); unset ⇒ the API shares the main pool.
        let api_database_url = optional("KATPOOL_API_DATABASE_URL");
        let stratum_port = require("KATPOOL_STRATUM_PORT", file.stratum_port.clone())?;
        let stratum_ports = parse_stratum_ports(
            optional("KATPOOL_STRATUM_PORTS")
                .or_else(|| file.stratum_ports.clone())
                .as_deref(),
        )?;
        let pool_address_raw = require("KATPOOL_POOL_ADDRESS", file.pool_address.clone())?;
        let pool_addresses = pool_address_raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                Address::try_from(s)
                    .map_err(|e| anyhow::anyhow!("KATPOOL_POOL_ADDRESS entry `{s}`: {e}"))
            })
            .collect::<Result<Vec<_>>>()?;
        if pool_addresses.is_empty() {
            anyhow::bail!("KATPOOL_POOL_ADDRESS produced an empty list");
        }
        let instance_id = optional("KATPOOL_INSTANCE_ID")
            .or_else(|| file.instance_id.clone())
            .unwrap_or_else(|| "katpool-runtime".to_owned());
        let prom_port = optional("KATPOOL_PROM_PORT")
            .or_else(|| file.prom_port.clone())
            .unwrap_or_default();
        let fee_topline_bps = optional_u16("KATPOOL_FEE_TOPLINE_BPS")?
            .or(file.fee_topline_bps)
            .unwrap_or(75);
        let min_share_diff = optional_u32("KATPOOL_MIN_SHARE_DIFF")?
            .or(file.min_share_diff)
            .unwrap_or(4096);
        let var_diff = optional_bool("KATPOOL_VAR_DIFF")?
            .or(file.var_diff)
            .unwrap_or(true);
        let shares_per_min = optional_u32("KATPOOL_SHARES_PER_MIN")?
            .or(file.shares_per_min)
            .unwrap_or(20);
        let proxy_protocol = optional_bool("KATPOOL_STRATUM_PROXY_PROTOCOL")?
            .or(file.proxy_protocol)
            .unwrap_or(false);
        let broadcast_capacity = optional_usize("KATPOOL_BROADCAST_CAPACITY")?
            .or(file.broadcast_capacity)
            .unwrap_or(4096);
        let poll_secs = optional_u64("KATPOOL_MATURITY_POLL_SECS")?
            .or(file.maturity_poll_secs)
            .unwrap_or(15);
        let coinbase_maturity = optional_u64("KATPOOL_COINBASE_MATURITY")?
            .or(file.coinbase_maturity)
            .unwrap_or(1000);
        let window_daa_span = optional_u64("KATPOOL_WINDOW_DAA_SPAN")?
            .or(file.window_daa_span)
            .unwrap_or(600);
        let batch_size = optional_i64("KATPOOL_MATURITY_BATCH_SIZE")?
            .or(file.maturity_batch_size)
            .unwrap_or(200);
        // Cutover DAA floor (0 = disabled): ignore coinbase UTXOs below this
        // DAA score so a prior pool's historical coinbases on a shared treasury
        // address are not re-discovered. Set at cutover (see Runbook 22).
        let coinbase_min_daa_score = optional_u64("KATPOOL_COINBASE_MIN_DAA_SCORE")?.unwrap_or(0);
        let network = resolve_network(
            &pool_addresses,
            optional("KATPOOL_NETWORK").or_else(|| file.network.clone()),
        )?;
        let event_record_path = optional("KATPOOL_EVENT_RECORD_PATH");

        // Public read-only API (Phase 6) + dedicated health port (ADR-0021).
        // Both accept `host:port` or `:port`; empty disables. The raw value is
        // taken env-first, then file, then parsed once.
        let api_bind = optional("KATPOOL_API_PORT")
            .or_else(|| file.api_port.clone())
            .map(|s| parse_bind_addr("KATPOOL_API_PORT", &s))
            .transpose()?;
        let health_check_raw =
            optional("KATPOOL_HEALTH_CHECK_PORT").or_else(|| file.health_check_port.clone());
        let health_check_port = health_check_raw.clone().unwrap_or_default();
        let health_bind = health_check_raw
            .map(|s| parse_bind_addr("KATPOOL_HEALTH_CHECK_PORT", &s))
            .transpose()?;
        let api_config =
            ApiConfig::from_env().map_err(|e| anyhow::anyhow!("API configuration: {e}"))?;

        let payout_enabled = optional_bool("KATPOOL_PAYOUT_ENABLED")?.unwrap_or(false);
        let payout_dry_run = optional_bool("KATPOOL_PAYOUT_DRY_RUN")?.unwrap_or(true);
        let payout_poll_secs = optional_u64("KATPOOL_PAYOUT_POLL_SECS")?.unwrap_or(60);
        let payout_cycle_span_daa =
            optional_u64("KATPOOL_PAYOUT_CYCLE_SPAN_DAA")?.unwrap_or(216_000);
        let payout_threshold_sompi = optional_i64("KATPOOL_PAYOUT_THRESHOLD_SOMPI")?
            .unwrap_or(DEFAULT_KAS_PAYOUT_THRESHOLD_SOMPI);
        let key_source = optional("KATPOOL_TREASURY_KEY_PATH").map_or_else(
            || {
                KeySource::SystemdCredential(
                    optional("KATPOOL_TREASURY_CREDENTIAL")
                        .unwrap_or_else(|| "treasury-key".to_owned()),
                )
            },
            |path| KeySource::File(PathBuf::from(path)),
        );
        let payout = PayoutConfig {
            dry_run: payout_dry_run,
            poll_interval: Duration::from_secs(payout_poll_secs),
            cycle_span_daa: payout_cycle_span_daa,
            threshold_sompi: payout_threshold_sompi,
            max_sompi_per_cycle: optional_i64("KATPOOL_PAYOUT_MAX_SOMPI_PER_CYCLE")?,
            key_source: key_source.clone(),
        };

        // Treasury UTXO consolidation engine (shares the treasury key source).
        // Off and dry-run by default. Hysteresis: sweeps start above
        // TRIGGER_UTXO_COUNT and compound down to TARGET_UTXO_COUNT. The per-tx
        // mempool standard-mass check is the real input guard, so
        // MAX_INPUTS_PER_TX is only an upper bound (~88 inputs actually fit).
        let consolidation_enabled =
            optional_bool("KATPOOL_CONSOLIDATION_ENABLED")?.unwrap_or(false);
        let consolidation = ConsolidationConfig {
            dry_run: optional_bool("KATPOOL_CONSOLIDATION_DRY_RUN")?.unwrap_or(true),
            poll_interval: Duration::from_secs(
                optional_u64("KATPOOL_CONSOLIDATION_POLL_SECS")?.unwrap_or(120),
            ),
            tick_timeout: Duration::from_secs(
                optional_u64("KATPOOL_CONSOLIDATION_TICK_TIMEOUT_SECS")?.unwrap_or(120),
            ),
            trigger_utxo_count: optional_usize("KATPOOL_CONSOLIDATION_TRIGGER_UTXO_COUNT")?
                .unwrap_or(1000),
            target_utxo_count: optional_usize("KATPOOL_CONSOLIDATION_TARGET_UTXO_COUNT")?
                .unwrap_or(50),
            max_inputs_per_tx: optional_usize("KATPOOL_CONSOLIDATION_MAX_INPUTS_PER_TX")?
                .unwrap_or(80),
            max_txs_per_tick: optional_usize("KATPOOL_CONSOLIDATION_MAX_TXS_PER_TICK")?
                .unwrap_or(50),
            key_source: key_source.clone(),
        };

        // KRC-20 NACHO payout engine (shares the treasury key source).
        let krc20_payout_enabled = optional_bool("KATPOOL_KRC20_PAYOUT_ENABLED")?.unwrap_or(false);
        let krc20_payout = Krc20RuntimeConfig::from_env(key_source)?;

        Ok(Self {
            kaspad_url,
            database_url,
            api_database_url,
            pool_addresses,
            stratum_port,
            stratum_ports,
            prom_port,
            health_check_port,
            instance_id,
            fee_topline_bps,
            min_share_diff,
            var_diff,
            shares_per_min,
            proxy_protocol,
            broadcast_capacity,
            network,
            event_record_path,
            geoip_db_path: optional("KATPOOL_GEOIP_DB"),
            api_bind,
            health_bind,
            api_config,
            maturity: MaturityConfig {
                poll_interval: Duration::from_secs(poll_secs),
                coinbase_maturity,
                window_daa_span,
                batch_size,
                coinbase_min_daa_score,
            },
            payout_enabled,
            payout,
            krc20_payout_enabled,
            krc20_payout,
            consolidation_enabled,
            consolidation,
            tier_classifier: TierClassifierKind::parse(
                optional("KATPOOL_TIER_CLASSIFIER")
                    .or_else(|| file.tier_classifier.clone())
                    .as_deref(),
            )?,
            shutdown_drain_idle: DEFAULT_SHUTDOWN_DRAIN_IDLE,
            shutdown_drain_budget: Duration::from_secs(
                optional_u64("KATPOOL_SHUTDOWN_DRAIN_SECS")?
                    .or(file.shutdown_drain_secs)
                    .unwrap_or(DEFAULT_SHUTDOWN_DRAIN_BUDGET_SECS),
            ),
        })
    }
}

/// A required configuration value, resolved env-first then file. Errors with
/// an actionable message naming both sources when neither supplies it.
fn require(var: &str, file_value: Option<String>) -> Result<String> {
    optional(var).or(file_value).ok_or_else(|| {
        anyhow::anyhow!(
            "required configuration `{var}` is unset (no environment variable and no config-file value)"
        )
    })
}

/// Parse `KATPOOL_STRATUM_PORTS` (ADR-0022): comma-separated `port:seed`
/// pairs, e.g. `1111:256,2222:1024`. `None`/empty => no multi-port
/// binding (single-port mode). Each port and seed is validated.
fn parse_stratum_ports(raw: Option<&str>) -> Result<Vec<(String, u32)>> {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(Vec::new());
    };
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|entry| {
            let (port, seed) = entry.split_once(':').ok_or_else(|| {
                anyhow::anyhow!("KATPOOL_STRATUM_PORTS entry `{entry}` must be `port:seed`")
            })?;
            let port = port.trim();
            port.parse::<u16>().map_err(|e| {
                anyhow::anyhow!("KATPOOL_STRATUM_PORTS entry `{entry}` has invalid port: {e}")
            })?;
            let seed = seed.trim().parse::<u32>().map_err(|e| {
                anyhow::anyhow!("KATPOOL_STRATUM_PORTS entry `{entry}` has invalid seed: {e}")
            })?;
            Ok((port.to_string(), seed))
        })
        .collect()
}

fn optional(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

fn optional_u16(var: &str) -> Result<Option<u16>> {
    optional(var)
        .map(|s| {
            s.parse::<u16>()
                .map_err(|e| anyhow::anyhow!("{var}=`{s}`: {e}"))
        })
        .transpose()
}

fn optional_u32(var: &str) -> Result<Option<u32>> {
    optional(var)
        .map(|s| {
            s.parse::<u32>()
                .map_err(|e| anyhow::anyhow!("{var}=`{s}`: {e}"))
        })
        .transpose()
}

fn optional_u64(var: &str) -> Result<Option<u64>> {
    optional(var)
        .map(|s| {
            s.parse::<u64>()
                .map_err(|e| anyhow::anyhow!("{var}=`{s}`: {e}"))
        })
        .transpose()
}

fn optional_i64(var: &str) -> Result<Option<i64>> {
    optional(var)
        .map(|s| {
            s.parse::<i64>()
                .map_err(|e| anyhow::anyhow!("{var}=`{s}`: {e}"))
        })
        .transpose()
}

fn optional_u128(var: &str) -> Result<Option<u128>> {
    optional(var)
        .map(|s| {
            s.parse::<u128>()
                .map_err(|e| anyhow::anyhow!("{var}=`{s}`: {e}"))
        })
        .transpose()
}

/// Parse a bind string as `host:port`, accepting a leading `:` (e.g. `:9301`)
/// as shorthand for `0.0.0.0:port` to match the `KATPOOL_PROM_PORT` operator
/// convention. `var` is only used to frame the error message.
fn parse_bind_addr(var: &str, s: &str) -> Result<SocketAddr> {
    let normalized = s
        .strip_prefix(':')
        .map_or_else(|| s.to_owned(), |port| format!("0.0.0.0:{port}"));
    normalized.parse::<SocketAddr>().map_err(|e| {
        anyhow::anyhow!("{var}=`{s}`: {e} (expected host:port, e.g. 127.0.0.1:8080, or :PORT)")
    })
}

fn optional_bool(var: &str) -> Result<Option<bool>> {
    optional(var)
        .map(|s| match s.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            other => Err(anyhow::anyhow!(
                "{var}=`{other}`: expected a boolean (true/false/1/0/yes/no/on/off)"
            )),
        })
        .transpose()
}

fn optional_usize(var: &str) -> Result<Option<usize>> {
    optional(var)
        .map(|s| {
            s.parse::<usize>()
                .map_err(|e| anyhow::anyhow!("{var}=`{s}`: {e}"))
        })
        .transpose()
}

/// Resolve the schema-network identifier for `wallet::ensure`.
///
/// Order of precedence:
/// 1. `KATPOOL_NETWORK` env override (required for `testnet-11`,
///    `devnet`, `simnet` because their bech32 prefixes overlap
///    other targets).
/// 2. Derived from the first pool address bech32 prefix — `kaspa:` →
///    `mainnet`, `kaspatest:` → `testnet-10` (the active testnet at
///    the time of writing; override via `KATPOOL_NETWORK` for
///    testnet-11).
///
/// The returned string is validated against
/// [`accountant::consumer::VALID_NETWORKS`] (matching the DB CHECK
/// constraint) so a misconfiguration fails fast on startup instead
/// of being discovered at the first `wallet::ensure` call.
/// Mirror the maturity tracker's kaspad-reachability signal into the API
/// [`ReadinessHandle`].
///
/// Each sweep publishes `true`/`false` on `sync_rx`; this task forwards that to
/// `kaspad_synced` and latches `started` the first time reachability is
/// observed. It exits when the tracker (the sender) is gone. This is the
/// "reuse existing kaspad polling, no second connection" wiring from ADR-0021.
fn spawn_readiness_bridge(mut sync_rx: watch::Receiver<bool>, readiness: ReadinessHandle) {
    tokio::spawn(async move {
        loop {
            let synced = *sync_rx.borrow_and_update();
            readiness.set_kaspad_synced(synced);
            if synced {
                readiness.mark_started();
            }
            if sync_rx.changed().await.is_err() {
                break;
            }
        }
    });
}

/// Append-only NDJSON capture of every `PoolEvent` on the bus.
fn spawn_event_recorder(mut rx: broadcast::Receiver<PoolEvent>, path: String) {
    tokio::spawn(async move {
        let mut file = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                error!(path = %path, error = %e, "event recorder: cannot open output file");
                return;
            }
        };
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let line = match serde_json::to_string(&event) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(error = %e, "event recorder: serialize failed");
                            continue;
                        }
                    };
                    if file.write_all(line.as_bytes()).await.is_err()
                        || file.write_all(b"\n").await.is_err()
                    {
                        error!(path = %path, "event recorder: write failed");
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(
                        skipped,
                        "event recorder lagged behind broadcast; events dropped"
                    );
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

fn resolve_network(pool_addresses: &[Address], network_override: Option<String>) -> Result<String> {
    let resolved = if let Some(override_value) = network_override {
        override_value
    } else {
        let first = pool_addresses
            .first()
            .ok_or_else(|| anyhow::anyhow!("resolve_network: pool_addresses empty"))?;
        match first.prefix {
            Prefix::Mainnet => "mainnet".to_owned(),
            Prefix::Testnet => "testnet-10".to_owned(),
            Prefix::Devnet => "devnet".to_owned(),
            Prefix::Simnet => "simnet".to_owned(),
        }
    };
    if !accountant::VALID_NETWORKS.contains(&resolved.as_str()) {
        anyhow::bail!(
            "KATPOOL_NETWORK=`{resolved}` not in {:?}",
            accountant::VALID_NETWORKS
        );
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::{
        Address, Command, parse_args, parse_bind_addr, parse_stratum_ports, resolve_network,
    };

    fn args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| (*s).to_owned()).collect()
    }

    // A real testnet-10 address; only its `kaspatest:` prefix matters here.
    const TN10_ADDR: &str =
        "kaspatest:qq5fysv96t636u4slda59daza6tn5j5p5x5953hs6dstajuw0u6l6ez5wz3gd";

    #[test]
    fn resolve_network_derives_from_address_prefix() -> anyhow::Result<()> {
        let addr = Address::try_from(TN10_ADDR)?;
        assert_eq!(resolve_network(&[addr], None)?, "testnet-10");
        Ok(())
    }

    #[test]
    fn resolve_network_prefers_explicit_override() -> anyhow::Result<()> {
        // The override (env-or-file `KATPOOL_NETWORK`) wins over the prefix.
        let addr = Address::try_from(TN10_ADDR)?;
        assert_eq!(
            resolve_network(&[addr], Some("testnet-11".to_owned()))?,
            "testnet-11"
        );
        Ok(())
    }

    #[test]
    fn resolve_network_rejects_unknown_override() -> anyhow::Result<()> {
        let addr = Address::try_from(TN10_ADDR)?;
        assert!(resolve_network(&[addr], Some("not-a-network".to_owned())).is_err());
        Ok(())
    }

    #[test]
    fn bind_addr_accepts_host_and_port() -> anyhow::Result<()> {
        let addr = parse_bind_addr("KATPOOL_API_PORT", "127.0.0.1:8080")?;
        assert_eq!(addr.to_string(), "127.0.0.1:8080");
        Ok(())
    }

    #[test]
    fn bind_addr_colon_port_means_all_interfaces() -> anyhow::Result<()> {
        let addr = parse_bind_addr("KATPOOL_HEALTH_CHECK_PORT", ":9301")?;
        assert_eq!(addr.to_string(), "0.0.0.0:9301");
        Ok(())
    }

    #[test]
    fn bind_addr_rejects_malformed() {
        assert!(parse_bind_addr("KATPOOL_API_PORT", "8080").is_err());
        assert!(parse_bind_addr("KATPOOL_API_PORT", "not-an-addr").is_err());
        assert!(parse_bind_addr("KATPOOL_API_PORT", ":notaport").is_err());
    }

    #[test]
    fn stratum_ports_empty_or_unset_is_single_port() -> anyhow::Result<()> {
        assert!(parse_stratum_ports(None)?.is_empty());
        assert!(parse_stratum_ports(Some(""))?.is_empty());
        assert!(parse_stratum_ports(Some("   "))?.is_empty());
        Ok(())
    }

    #[test]
    fn stratum_ports_parses_pairs() -> anyhow::Result<()> {
        let parsed = parse_stratum_ports(Some("1111:256, 8888:2048"))?;
        assert_eq!(
            parsed,
            vec![("1111".to_string(), 256), ("8888".to_string(), 2048)]
        );
        Ok(())
    }

    #[test]
    fn stratum_ports_rejects_malformed_entries() {
        assert!(parse_stratum_ports(Some("1111")).is_err());
        assert!(parse_stratum_ports(Some("notaport:256")).is_err());
        assert!(parse_stratum_ports(Some("1111:notaseed")).is_err());
        assert!(parse_stratum_ports(Some("70000:256")).is_err());
    }

    #[test]
    fn no_args_runs_daemon() {
        assert_eq!(parse_args(&args(&[])).ok(), Some(Command::Daemon));
    }

    #[test]
    fn help_flags_request_usage() {
        for flag in ["-h", "--help", "help"] {
            assert_eq!(parse_args(&args(&[flag])).ok(), Some(Command::Help));
        }
    }

    #[test]
    fn payout_run_now_defaults_to_live() {
        assert_eq!(
            parse_args(&args(&["payout", "run-now"])).ok(),
            Some(Command::PayoutRunNow { dry_run: false })
        );
    }

    #[test]
    fn payout_run_now_accepts_dry_run_flag() {
        assert_eq!(
            parse_args(&args(&["payout", "run-now", "--dry-run"])).ok(),
            Some(Command::PayoutRunNow { dry_run: true })
        );
    }

    #[test]
    fn unknown_payout_subcommand_errors() {
        assert!(parse_args(&args(&["payout", "bogus"])).is_err());
    }

    #[test]
    fn payout_without_subcommand_errors() {
        assert!(parse_args(&args(&["payout"])).is_err());
    }

    #[test]
    fn unknown_flag_for_run_now_errors() {
        assert!(parse_args(&args(&["payout", "run-now", "--wat"])).is_err());
    }

    #[test]
    fn unknown_top_level_command_errors() {
        assert!(parse_args(&args(&["frobnicate"])).is_err());
    }
}
