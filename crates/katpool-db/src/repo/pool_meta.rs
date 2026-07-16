//! Pool-meta aggregate — single-row key/value store for runtime
//! constants the application needs to persist across restarts.
//!
//! Used by the accountant for `last_daa_processed`-style markers, by
//! the API for warmup timestamps, by the importer for `import_*`
//! resumption cursors. **Not** for configuration — environment vars
//! and YAML own that.

use chrono::{DateTime, Utc};
use sqlx::PgExecutor;

use crate::DbError;

/// One row of the `pool_meta` table.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PoolMetaEntry {
    /// Lookup key.
    pub key: String,
    /// Free-form value.
    pub value: String,
    /// Updated-at timestamp; refreshed on every [`set`].
    pub updated_at: DateTime<Utc>,
}

/// Read the value for a key. Returns `Ok(None)` if missing — the
/// table is intentionally sparse and the caller is expected to
/// distinguish "not yet set" from "empty string".
pub async fn get<'e, E: PgExecutor<'e>>(
    executor: E,
    key: &str,
) -> Result<Option<PoolMetaEntry>, DbError> {
    sqlx::query_as::<_, PoolMetaEntry>(
        "SELECT key, value, updated_at FROM pool_meta WHERE key = $1",
    )
    .bind(key)
    .fetch_optional(executor)
    .await
    .map_err(DbError::from)
}

/// Idempotent upsert. Refreshes `updated_at` on every call.
pub async fn set<'e, E: PgExecutor<'e>>(
    executor: E,
    key: &str,
    value: &str,
) -> Result<PoolMetaEntry, DbError> {
    sqlx::query_as::<_, PoolMetaEntry>(
        "INSERT INTO pool_meta (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value,
                updated_at = now()
         RETURNING key, value, updated_at",
    )
    .bind(key)
    .bind(value)
    .fetch_one(executor)
    .await
    .map_err(DbError::from)
}

/// Delete a key. Idempotent: returns `true` iff a row was removed.
/// Used by latch-style keys (e.g. the ZKas payout in-flight guard).
pub async fn delete<'e, E: PgExecutor<'e>>(executor: E, key: &str) -> Result<bool, DbError> {
    let result = sqlx::query("DELETE FROM pool_meta WHERE key = $1")
        .bind(key)
        .execute(executor)
        .await
        .map_err(DbError::from)?;
    Ok(result.rows_affected() > 0)
}
