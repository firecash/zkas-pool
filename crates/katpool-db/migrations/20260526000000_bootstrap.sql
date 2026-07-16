-- katpool bootstrap migration.
--
-- Creates the entire schema in a single transaction. See ADR-0011
-- for the design rationale and ADR-0006 for the postgres-17 pin.
-- Conventions:
--   * Every non-static table has at least one foreign key.
--   * Every monetary column is `BIGINT` (sompi); never `numeric`.
--   * Every hash column is `BYTEA` with `CHECK octet_length = 32`.
--   * Every timestamp column is `TIMESTAMPTZ`.
--   * State machines use postgres `ENUM` types, never free-form text.
--   * Synthetic primary keys are `BIGSERIAL`; UUIDs are reserved for
--     correlation ids generated outside the database (the bridge's
--     event-bus `PoolEvent`).

BEGIN;

-- ---------------------------------------------------------------
-- Enum types (state machines). Variant additions are explicit
-- subsequent migrations; we never edit these in place.
-- ---------------------------------------------------------------

CREATE TYPE block_status AS ENUM (
    'found',                -- bridge detected PoW met network target
    'submitted_to_node',    -- kaspad acknowledged submit_block
    'confirmed_blue',       -- block confirmed blue in DAG
    'matured',              -- coinbase matured (UtxoProcessor observed)
    'orphaned'              -- displaced by a DAG re-org
);

CREATE TYPE payout_kind AS ENUM (
    'kas',
    'krc20_nacho'
);

CREATE TYPE payout_cycle_status AS ENUM (
    'planned',              -- allocations computed, not broadcast
    'broadcasting',         -- transactions in flight
    'partially_settled',    -- some confirmed, others pending
    'settled',              -- every recipient payout confirmed
    'failed'                -- broadcast errored, needs investigation
);

CREATE TYPE payout_status AS ENUM (
    'planned',
    'submitted',            -- in mempool
    'accepted',             -- accepted by network (first confirmation)
    'confirmed',            -- finalised past maturity window
    'failed'
);

CREATE TYPE krc20_transfer_status AS ENUM (
    'pending',              -- commit tx not yet submitted
    'commit_submitted',     -- commit tx on chain
    'reveal_submitted',     -- reveal tx on chain
    'completed',            -- both confirmed
    'failed'
);

-- ---------------------------------------------------------------
-- wallet — wallet identity. 1 row per wallet ever seen.
-- ---------------------------------------------------------------

CREATE TABLE wallet (
    id              BIGSERIAL PRIMARY KEY,
    address         TEXT NOT NULL UNIQUE,
    network         TEXT NOT NULL,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT wallet_network_valid
        CHECK (network IN ('mainnet', 'testnet-10', 'testnet-11', 'devnet', 'simnet')),
    -- ZKas HRPs are canonical (zkas: family), the pre-rebrand firecash: family
    -- is an accepted legacy alias, and the upstream kaspa: family is kept so
    -- upstream-derived fixtures/tests still insert. A ZKas shielded (Orchard)
    -- address body is ~79 chars; kaspa schnorr bodies ~61 — hence {40,100}.
    -- Keep in lock-step with katpool_domain::address::ACCEPTED_PREFIXES.
    CONSTRAINT wallet_address_format
        CHECK (
            (network = 'mainnet'    AND address ~ '^(zkas|firecash|kaspa):[a-z0-9]{40,100}$') OR
            (network = 'testnet-10' AND address ~ '^(zkastest|firecashtest|kaspatest):[a-z0-9]{40,100}$') OR
            (network = 'testnet-11' AND address ~ '^(zkastest|firecashtest|kaspatest):[a-z0-9]{40,100}$') OR
            (network = 'devnet'     AND address ~ '^(zkasdev|firecashdev|kaspadev):[a-z0-9]{40,100}$') OR
            (network = 'simnet'     AND address ~ '^(zkassim|firecashsim|kaspasim):[a-z0-9]{40,100}$')
        )
);

CREATE INDEX idx_wallet_last_seen ON wallet (last_seen_at DESC);

-- ---------------------------------------------------------------
-- worker — rig identity within a wallet.
-- ---------------------------------------------------------------

CREATE TABLE worker (
    id              BIGSERIAL PRIMARY KEY,
    wallet_id       BIGINT NOT NULL REFERENCES wallet(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (wallet_id, name),
    CONSTRAINT worker_name_length CHECK (length(name) BETWEEN 1 AND 64),
    -- Matches katpool_domain::WorkerName allowed character set.
    CONSTRAINT worker_name_charset CHECK (name ~ '^[A-Za-z0-9_\-.#@:+/\\]+$')
);

CREATE INDEX idx_worker_wallet ON worker (wallet_id);

-- ---------------------------------------------------------------
-- connection_session — every stratum TCP session.
-- ---------------------------------------------------------------

CREATE TABLE connection_session (
    id                  BIGSERIAL PRIMARY KEY,
    -- A session can pre-date authorize, so worker_id is nullable; the
    -- accountant fills it in when the first ShareCredited arrives.
    worker_id           BIGINT REFERENCES worker(id) ON DELETE SET NULL,
    remote_ip           INET NOT NULL,
    remote_app          TEXT,
    connected_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    disconnected_at     TIMESTAMPTZ,
    shares_credited     BIGINT NOT NULL DEFAULT 0 CHECK (shares_credited >= 0),
    shares_rejected     BIGINT NOT NULL DEFAULT 0 CHECK (shares_rejected >= 0),
    malformed_frames    BIGINT NOT NULL DEFAULT 0 CHECK (malformed_frames >= 0)
);

CREATE INDEX idx_session_worker_connected ON connection_session (worker_id, connected_at DESC);
CREATE INDEX idx_session_ip_connected ON connection_session (remote_ip, connected_at DESC);

-- ---------------------------------------------------------------
-- share — every accepted share. PROP allocation reads this table
-- heavily, so the (daa_score, wallet_id) index is the primary
-- access path.
-- ---------------------------------------------------------------

CREATE TABLE share (
    id              BIGSERIAL PRIMARY KEY,
    wallet_id       BIGINT NOT NULL REFERENCES wallet(id) ON DELETE RESTRICT,
    worker_id       BIGINT NOT NULL REFERENCES worker(id) ON DELETE RESTRICT,
    session_id      BIGINT REFERENCES connection_session(id) ON DELETE SET NULL,
    difficulty      DOUBLE PRECISION NOT NULL CHECK (difficulty > 0 AND difficulty = difficulty),
    daa_score       BIGINT NOT NULL CHECK (daa_score >= 0),
    credited_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    correlation_id  UUID NOT NULL
);

-- PROP-window scan path.
CREATE INDEX idx_share_daa_wallet ON share (daa_score, wallet_id);
-- Per-worker analytics path (recent activity, hash-rate gauges).
CREATE INDEX idx_share_worker_credited ON share (worker_id, credited_at DESC);

-- ---------------------------------------------------------------
-- share_window — pre-aggregated PROP rollups for closed windows.
-- ---------------------------------------------------------------

CREATE TABLE share_window (
    id              BIGSERIAL PRIMARY KEY,
    wallet_id       BIGINT NOT NULL REFERENCES wallet(id) ON DELETE RESTRICT,
    -- Half-open DAA range [daa_start, daa_end).
    daa_start       BIGINT NOT NULL,
    daa_end         BIGINT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ NOT NULL,
    -- sum(share.difficulty) over the window — the PROP weight.
    total_weight    DOUBLE PRECISION NOT NULL CHECK (total_weight >= 0),
    share_count     BIGINT NOT NULL CHECK (share_count >= 0),
    UNIQUE (wallet_id, daa_start, daa_end),
    CONSTRAINT share_window_range CHECK (daa_end > daa_start)
);

CREATE INDEX idx_share_window_daa ON share_window (daa_start, daa_end);

-- ---------------------------------------------------------------
-- block — blocks we found, full lifecycle.
-- ---------------------------------------------------------------

CREATE TABLE block (
    id                  BIGSERIAL PRIMARY KEY,
    hash                BYTEA NOT NULL UNIQUE CHECK (octet_length(hash) = 32),
    finder_wallet_id    BIGINT NOT NULL REFERENCES wallet(id) ON DELETE RESTRICT,
    finder_worker_id    BIGINT NOT NULL REFERENCES worker(id) ON DELETE RESTRICT,
    daa_score           BIGINT NOT NULL CHECK (daa_score >= 0),
    blue_score          BIGINT CHECK (blue_score IS NULL OR blue_score >= 0),
    nonce               BIGINT NOT NULL,
    status              block_status NOT NULL DEFAULT 'found',
    found_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at        TIMESTAMPTZ,
    confirmed_at        TIMESTAMPTZ,
    matured_at          TIMESTAMPTZ,
    miner_reward_sompi  BIGINT CHECK (miner_reward_sompi IS NULL OR miner_reward_sompi >= 0),
    correlation_id      UUID NOT NULL,
    -- Status transitions are monotone: timestamps must agree with the
    -- declared status. CHECK enforces non-decreasing fill order.
    CONSTRAINT block_lifecycle_order CHECK (
        (submitted_at IS NULL OR submitted_at >= found_at) AND
        (confirmed_at IS NULL OR (submitted_at IS NOT NULL AND confirmed_at >= submitted_at)) AND
        (matured_at IS NULL OR (confirmed_at IS NOT NULL AND matured_at >= confirmed_at))
    )
);

CREATE INDEX idx_block_status_found ON block (status, found_at DESC);
CREATE INDEX idx_block_finder ON block (finder_wallet_id, found_at DESC);
CREATE INDEX idx_block_daa ON block (daa_score);

-- ---------------------------------------------------------------
-- share_allocation — PROP allocation of a block's reward among
-- the wallets that contributed shares to its window.
-- ---------------------------------------------------------------

CREATE TABLE share_allocation (
    id                      BIGSERIAL PRIMARY KEY,
    block_id                BIGINT NOT NULL REFERENCES block(id) ON DELETE CASCADE,
    wallet_id               BIGINT NOT NULL REFERENCES wallet(id) ON DELETE RESTRICT,
    weight                  DOUBLE PRECISION NOT NULL CHECK (weight >= 0),
    window_total            DOUBLE PRECISION NOT NULL CHECK (window_total > 0),
    -- gross = round(miner_reward * weight / window_total)
    gross_share_sompi       BIGINT NOT NULL CHECK (gross_share_sompi >= 0),
    pool_fee_sompi          BIGINT NOT NULL CHECK (pool_fee_sompi >= 0),
    nacho_accrual_sompi     BIGINT NOT NULL CHECK (nacho_accrual_sompi >= 0),
    net_payout_sompi        BIGINT NOT NULL CHECK (net_payout_sompi >= 0),
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (block_id, wallet_id),
    CONSTRAINT share_allocation_balance
        CHECK (gross_share_sompi = pool_fee_sompi + nacho_accrual_sompi + net_payout_sompi),
    CONSTRAINT share_allocation_weight_le_total
        CHECK (weight <= window_total)
);

CREATE INDEX idx_share_allocation_wallet ON share_allocation (wallet_id, computed_at DESC);

-- ---------------------------------------------------------------
-- payout_cycle — one cycle per kind, idempotent by name.
-- ---------------------------------------------------------------

CREATE TABLE payout_cycle (
    id                  BIGSERIAL PRIMARY KEY,
    kind                payout_kind NOT NULL,
    status              payout_cycle_status NOT NULL DEFAULT 'planned',
    daa_start           BIGINT NOT NULL CHECK (daa_start >= 0),
    daa_end             BIGINT NOT NULL,
    planned_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    broadcast_at        TIMESTAMPTZ,
    settled_at          TIMESTAMPTZ,
    total_sompi         BIGINT NOT NULL DEFAULT 0 CHECK (total_sompi >= 0),
    total_recipients    INTEGER NOT NULL DEFAULT 0 CHECK (total_recipients >= 0),
    idempotency_key     TEXT NOT NULL UNIQUE,
    CONSTRAINT payout_cycle_range CHECK (daa_end > daa_start),
    CONSTRAINT payout_cycle_order CHECK (
        (broadcast_at IS NULL OR broadcast_at >= planned_at) AND
        (settled_at IS NULL OR (broadcast_at IS NOT NULL AND settled_at >= broadcast_at))
    )
);

CREATE INDEX idx_payout_cycle_status ON payout_cycle (status, planned_at DESC);
CREATE INDEX idx_payout_cycle_kind_recent ON payout_cycle (kind, planned_at DESC);

-- ---------------------------------------------------------------
-- payout — individual recipient payout under a cycle.
-- ---------------------------------------------------------------

CREATE TABLE payout (
    id                  BIGSERIAL PRIMARY KEY,
    cycle_id            BIGINT NOT NULL REFERENCES payout_cycle(id) ON DELETE RESTRICT,
    wallet_id           BIGINT NOT NULL REFERENCES wallet(id) ON DELETE RESTRICT,
    amount_sompi        BIGINT NOT NULL CHECK (amount_sompi > 0),
    status              payout_status NOT NULL DEFAULT 'planned',
    tx_hash             BYTEA CHECK (tx_hash IS NULL OR octet_length(tx_hash) = 32),
    krc20_commit_hash   BYTEA CHECK (krc20_commit_hash IS NULL OR octet_length(krc20_commit_hash) = 32),
    krc20_reveal_hash   BYTEA CHECK (krc20_reveal_hash IS NULL OR octet_length(krc20_reveal_hash) = 32),
    planned_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at        TIMESTAMPTZ,
    confirmed_at        TIMESTAMPTZ,
    failure_reason      TEXT,
    UNIQUE (cycle_id, wallet_id),
    CONSTRAINT payout_lifecycle_order CHECK (
        (submitted_at IS NULL OR submitted_at >= planned_at) AND
        (confirmed_at IS NULL OR (submitted_at IS NOT NULL AND confirmed_at >= submitted_at))
    )
);

CREATE INDEX idx_payout_status ON payout (status, planned_at DESC);
CREATE INDEX idx_payout_wallet ON payout (wallet_id, planned_at DESC);

-- ---------------------------------------------------------------
-- nacho_rebate_accrual — running NACHO rebate balance per wallet.
-- ---------------------------------------------------------------

CREATE TABLE nacho_rebate_accrual (
    wallet_id           BIGINT PRIMARY KEY REFERENCES wallet(id) ON DELETE RESTRICT,
    accrued_sompi       BIGINT NOT NULL DEFAULT 0 CHECK (accrued_sompi >= 0),
    paid_sompi          BIGINT NOT NULL DEFAULT 0 CHECK (paid_sompi >= 0),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT nacho_rebate_paid_le_accrued CHECK (paid_sompi <= accrued_sompi)
);

-- ---------------------------------------------------------------
-- krc20_pending_transfer — KRC-20 commit/reveal state machine.
-- ---------------------------------------------------------------

CREATE TABLE krc20_pending_transfer (
    id                  BIGSERIAL PRIMARY KEY,
    payout_id           BIGINT NOT NULL UNIQUE REFERENCES payout(id) ON DELETE CASCADE,
    sompi_to_miner      BIGINT NOT NULL CHECK (sompi_to_miner > 0),
    nacho_amount        BIGINT NOT NULL CHECK (nacho_amount > 0),
    p2sh_address        TEXT NOT NULL,
    status              krc20_transfer_status NOT NULL DEFAULT 'pending',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_krc20_status ON krc20_pending_transfer (status, created_at DESC);

-- ---------------------------------------------------------------
-- treasury_snapshot — periodic hot-wallet snapshots.
-- ---------------------------------------------------------------

CREATE TABLE treasury_snapshot (
    id                  BIGSERIAL PRIMARY KEY,
    captured_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    kas_balance_sompi   BIGINT NOT NULL CHECK (kas_balance_sompi >= 0),
    nacho_balance       BIGINT NOT NULL CHECK (nacho_balance >= 0),
    daa_score           BIGINT NOT NULL CHECK (daa_score >= 0),
    blue_score          BIGINT NOT NULL CHECK (blue_score >= 0),
    notes               TEXT
);

CREATE INDEX idx_treasury_snapshot_captured ON treasury_snapshot (captured_at DESC);

-- ---------------------------------------------------------------
-- audit_log — append-only audit trail.
-- ---------------------------------------------------------------

CREATE TABLE audit_log (
    id              BIGSERIAL PRIMARY KEY,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor           TEXT NOT NULL,
    action          TEXT NOT NULL,
    subject_type    TEXT,
    subject_id      BIGINT,
    correlation_id  UUID,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_audit_log_occurred ON audit_log (occurred_at DESC);
CREATE INDEX idx_audit_log_subject ON audit_log (subject_type, subject_id);
CREATE INDEX idx_audit_log_action ON audit_log (action, occurred_at DESC);

-- ---------------------------------------------------------------
-- pool_meta — single-row key/value for runtime constants.
-- ---------------------------------------------------------------

CREATE TABLE pool_meta (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed the schema-version row so an operator can `SELECT * FROM pool_meta`
-- without joining `_sqlx_migrations`. Updated by every subsequent
-- migration via `INSERT ... ON CONFLICT`.
INSERT INTO pool_meta (key, value) VALUES
    ('schema_bootstrap_migration', '20260526000000_bootstrap');

COMMIT;
