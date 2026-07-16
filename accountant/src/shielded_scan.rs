//! Shielded (ZKas) reward discovery — the [`KaspadClient`] implementation
//! for a chain whose coinbase is an **Orchard note**, not a transparent UTXO.
//!
//! On ZKas there is no UTXO index entry for the pool's reward:
//! `get_utxos_by_addresses` (the upstream [`crate::kaspad_grpc::KaspadGrpcClient`]
//! path) returns nothing, ever. Instead, every chain block's coinbase
//! *publicly states* its recipients and values — a coinbase note commitment is
//! deterministically recomputed by consensus from `(recipient, value, ρ, rseed)`
//! (see `shielded-core/src/coinbase.rs` in the node fork) — so discovering the
//! pool treasury's rewards needs **no viewing key at all**: walk the selected
//! chain via `GetShieldedBlocks` and keep the coinbase outputs whose 43-byte
//! Orchard recipient equals the treasury's address payload.
//!
//! ## Mapping onto the existing tracker
//!
//! The scanner implements the same three-method [`KaspadClient`] surface the
//! [`MaturityTracker`](crate::maturity::MaturityTracker) already consumes, so
//! the tracker, the PROP allocation engine, and the `coinbase_reward` table
//! are reused unchanged:
//!
//! - one matched coinbase output becomes one [`CoinbaseUtxo`] with
//!   `transaction_id = coinbase_txid`, `index = output position`,
//!   `amount_sompi = the public value`, `block_daa_score = the minting chain
//!   block's DAA score` — exactly the identity the schema's
//!   `UNIQUE (outpoint_transaction_id, outpoint_index)` expects;
//! - `coinbase_reward::ensure` is idempotent by that outpoint, so rescans
//!   (restart from an old cursor, reorg reset) are always safe.
//!
//! ## Cursor + maturity hold-back
//!
//! `GetShieldedBlocks` is a resume-cursor stream over *chain* blocks. The
//! scanner persists its cursor in `pool_meta` (key
//! [`SHIELDED_SCAN_CURSOR_KEY`]) so a restart resumes instead of rescanning
//! the whole pruning window. Two rules keep it lossless:
//!
//! 1. **Never advance the cursor past an immature block.** The tracker
//!    silently skips immature rewards each sweep and relies on seeing them
//!    again later; a cursor stream would drop them forever. So the scanner
//!    only ingests blocks at least `maturity_margin` DAA below the current
//!    virtual DAA score — everything it returns is already mature by the
//!    tracker's own gate — and stops (holding the cursor) at the first block
//!    inside the margin.
//! 2. **On `reorged`, reset to the pruning point.** The response flag means
//!    the cursor block left the selected chain; the pruning point is always
//!    canonical, and idempotent `ensure` makes the rescan harmless.
//!
//! First run (no cursor row) also starts from the pruning point. Combined
//! with the tracker's `coinbase_min_daa_score` cutover floor, pre-cutover
//! coinbases found by that initial walk are ignored rather than allocated.

#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use kaspa_addresses::{Address, Version};
use kaspa_grpc_client::GrpcClient;
use kaspa_hashes::Hash as KaspaHash;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::model::message::RpcShieldedChainBlock;
use katpool_db::repo::pool_meta;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::kaspad_grpc::KaspadGrpcClient;
use crate::maturity::{BlockColor, CoinbaseUtxo, KaspadClient, KaspadError};

/// `pool_meta` key holding the scan cursor (a chain-block hash, hex).
pub const SHIELDED_SCAN_CURSOR_KEY: &str = "shielded_scan_cursor";

/// Chain blocks requested per `GetShieldedBlocks` page.
pub const DEFAULT_PAGE_LIMIT: u64 = 1_000;

/// Pages consumed per sweep. Bounds a single tracker sweep's duration when
/// catching up (first run / long downtime); the remainder is picked up by
/// the next sweep. 50 pages × 1000 blocks ≈ 14 h of chain at 1 BPS.
pub const DEFAULT_MAX_PAGES_PER_SWEEP: u64 = 50;

/// Extra DAA depth (beyond consensus coinbase maturity) a block must have
/// before the scanner ingests it. Defence-in-depth against DAA-vs-depth
/// skew: everything returned must already pass the tracker's `is_mature`
/// gate, or it would be skipped *and* lost behind the cursor.
pub const DEFAULT_MATURITY_SAFETY_DAA: u64 = 50;

/// [`KaspadClient`] for shielded-coinbase (ZKas) chains.
///
/// Colour and DAA queries delegate to the shared gRPC surface (identical to
/// the transparent client); only reward discovery differs.
pub struct ShieldedRewardScanner {
    client: Arc<GrpcClient>,
    db: PgPool,
    /// Raw 43-byte Orchard recipients of the treasury address(es).
    treasury_recipients: Vec<Vec<u8>>,
    /// Consensus coinbase maturity in DAA (the tracker uses the same value).
    coinbase_maturity: u64,
    page_limit: u64,
    max_pages_per_sweep: u64,
    /// Delegate for the two methods shared with the transparent client.
    colour: KaspadGrpcClient,
}

/// Errors from [`ShieldedRewardScanner::new`].
#[derive(Debug, thiserror::Error)]
pub enum ScannerConfigError {
    /// A configured pool address is not a shielded (Orchard) address.
    #[error("pool address `{address}` is not a shielded (Orchard) address")]
    NotShielded {
        /// Redacted offending address.
        address: String,
    },
    /// No pool addresses were supplied.
    #[error("no pool addresses supplied")]
    Empty,
}

impl ShieldedRewardScanner {
    /// Construct a scanner for the given treasury address(es).
    ///
    /// Every address must be [`Version::ShieldedOrchard`] — a transparent
    /// address here means the operator wired the wrong client and reward
    /// discovery would silently find nothing.
    pub fn new(
        client: Arc<GrpcClient>,
        db: PgPool,
        pool_addresses: &[Address],
        coinbase_maturity: u64,
    ) -> Result<Self, ScannerConfigError> {
        if pool_addresses.is_empty() {
            return Err(ScannerConfigError::Empty);
        }
        let mut treasury_recipients = Vec::with_capacity(pool_addresses.len());
        for a in pool_addresses {
            if a.version != Version::ShieldedOrchard {
                return Err(ScannerConfigError::NotShielded {
                    address: katpool_domain::redact::address(&a.to_string()),
                });
            }
            treasury_recipients.push(a.payload.as_slice().to_vec());
        }
        let colour = KaspadGrpcClient::new(Arc::clone(&client), pool_addresses.to_vec());
        Ok(Self {
            client,
            db,
            treasury_recipients,
            coinbase_maturity,
            page_limit: DEFAULT_PAGE_LIMIT,
            max_pages_per_sweep: DEFAULT_MAX_PAGES_PER_SWEEP,
            colour,
        })
    }

    /// The DAA score at or below which a block is safely ingestible: deep
    /// enough that the tracker's maturity gate passes with margin.
    const fn safe_ingest_daa(&self, virtual_daa_score: u64) -> u64 {
        virtual_daa_score
            .saturating_sub(self.coinbase_maturity)
            .saturating_sub(DEFAULT_MATURITY_SAFETY_DAA)
    }

    async fn load_cursor(&self) -> Result<Option<KaspaHash>, KaspadError> {
        let row = pool_meta::get(&self.db, SHIELDED_SCAN_CURSOR_KEY)
            .await
            .map_err(|e| KaspadError::Transport(format!("cursor read: {e}")))?;
        match row {
            None => Ok(None),
            Some(entry) => KaspaHash::from_str(&entry.value)
                .map(Some)
                .map_err(|e| KaspadError::Malformed(format!("stored cursor `{}`: {e}", entry.value))),
        }
    }

    async fn store_cursor(&self, hash: KaspaHash) -> Result<(), KaspadError> {
        pool_meta::set(&self.db, SHIELDED_SCAN_CURSOR_KEY, &hash.to_string())
            .await
            .map(|_| ())
            .map_err(|e| KaspadError::Transport(format!("cursor write: {e}")))
    }

    async fn pruning_point(&self) -> Result<KaspaHash, KaspadError> {
        self.client
            .get_block_dag_info()
            .await
            .map(|info| info.pruning_point_hash)
            .map_err(|e| KaspadError::Transport(format!("{e}")))
    }
}

/// Project the treasury-matching coinbase outputs of ingestible blocks.
///
/// Walks `blocks` (oldest first, as `GetShieldedBlocks` returns them),
/// ingesting every block whose `daa_score <= safe_ingest_daa` and stopping
/// at the first block above it (that block and everything after stay ahead
/// of the cursor for a later sweep). Returns the matched rewards and the
/// hash of the last ingested block (`None` if nothing was ingestible).
///
/// Pure function, factored out for unit testing.
#[must_use]
pub fn extract_treasury_rewards(
    blocks: &[RpcShieldedChainBlock],
    treasury_recipients: &[Vec<u8>],
    safe_ingest_daa: u64,
) -> (Vec<CoinbaseUtxo>, Option<KaspaHash>) {
    let mut rewards = Vec::new();
    let mut last_ingested = None;
    for b in blocks {
        if b.daa_score > safe_ingest_daa {
            break;
        }
        for (index, out) in b.coinbase_outputs.iter().enumerate() {
            if treasury_recipients
                .iter()
                .any(|r| r.as_slice() == out.script_public_key.as_slice())
            {
                let Ok(amount) = i64::try_from(out.value) else {
                    // Guarded upstream by the emission schedule; skip rather
                    // than wrap.
                    continue;
                };
                rewards.push(CoinbaseUtxo {
                    transaction_id: b.coinbase_txid.as_bytes(),
                    index: index as u32,
                    amount_sompi: amount as u64,
                    block_daa_score: b.daa_score,
                });
            }
        }
        last_ingested = Some(b.hash);
    }
    (rewards, last_ingested)
}

#[async_trait]
impl KaspadClient for ShieldedRewardScanner {
    async fn get_virtual_daa_score(&self) -> Result<u64, KaspadError> {
        self.colour.get_virtual_daa_score().await
    }

    async fn get_block_color(&self, hash: katpool_domain::BlockHash) -> Result<BlockColor, KaspadError> {
        self.colour.get_block_color(hash).await
    }

    /// Incremental scan of the selected chain for treasury coinbase mints.
    ///
    /// Unlike the transparent client (which returns the *entire current*
    /// UTXO set each call), this returns the rewards discovered **since the
    /// persisted cursor** — the tracker's `coinbase_reward::ensure` is
    /// idempotent by outpoint, so both shapes are equivalent to it, and
    /// everything returned is mature by construction (see module docs).
    async fn get_pool_coinbase_utxos(&self) -> Result<Vec<CoinbaseUtxo>, KaspadError> {
        let virtual_daa_score = self.get_virtual_daa_score().await?;
        let safe_daa = self.safe_ingest_daa(virtual_daa_score);

        let mut cursor = match self.load_cursor().await? {
            Some(h) => h,
            None => {
                let pp = self.pruning_point().await?;
                info!(cursor = %pp, "shielded scan: no cursor; starting from pruning point");
                pp
            }
        };

        let mut all_rewards = Vec::new();
        for _page in 0..self.max_pages_per_sweep {
            let resp = self
                .client
                .get_shielded_blocks(cursor, self.page_limit)
                .await
                .map_err(|e| KaspadError::Transport(format!("get_shielded_blocks: {e}")))?;

            if resp.reorged {
                // Cursor left the selected chain. Reset to the pruning point
                // (always canonical); the rescan is safe because reward
                // recording is idempotent by outpoint.
                let pp = self.pruning_point().await?;
                warn!(stale = %cursor, reset = %pp, "shielded scan: cursor reorged; resetting to pruning point");
                self.store_cursor(pp).await?;
                return Ok(all_rewards);
            }
            if resp.blocks.is_empty() {
                break;
            }

            let (mut rewards, last_ingested) =
                extract_treasury_rewards(&resp.blocks, &self.treasury_recipients, safe_daa);
            all_rewards.append(&mut rewards);

            match last_ingested {
                Some(h) => {
                    self.store_cursor(h).await?;
                    cursor = h;
                    // If the page was cut short by the maturity hold-back,
                    // the rest isn't ingestible yet — stop for this sweep.
                    if resp
                        .blocks
                        .last()
                        .is_some_and(|b| b.daa_score > safe_daa)
                    {
                        break;
                    }
                }
                // First block of the page is already inside the hold-back
                // margin: nothing more to do this sweep.
                None => break,
            }
        }

        debug!(
            rewards = all_rewards.len(),
            cursor = %cursor,
            virtual_daa_score,
            "shielded scan sweep done"
        );
        Ok(all_rewards)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]
    use kaspa_rpc_core::model::message::RpcShieldedCoinbaseOutput;

    use super::*;

    const TREASURY: [u8; 43] = [7u8; 43];
    const OTHER: [u8; 43] = [9u8; 43];

    fn block(
        tag: u8,
        daa: u64,
        outputs: Vec<(&[u8], u64)>,
    ) -> RpcShieldedChainBlock {
        RpcShieldedChainBlock {
            hash: KaspaHash::from_bytes([tag; 32]),
            blue_score: daa,
            daa_score: daa,
            coinbase_txid: KaspaHash::from_bytes([tag ^ 0xFF; 32]),
            coinbase_outputs: outputs
                .into_iter()
                .map(|(spk, value)| RpcShieldedCoinbaseOutput {
                    script_public_key: spk.to_vec(),
                    value,
                })
                .collect(),
            accepted_bundles: vec![],
        }
    }

    fn recipients() -> Vec<Vec<u8>> {
        vec![TREASURY.to_vec()]
    }

    #[test]
    fn matches_only_treasury_outputs() {
        let blocks = vec![block(1, 100, vec![(&OTHER, 50), (&TREASURY, 60), (&TREASURY, 7)])];
        let (rewards, last) = extract_treasury_rewards(&blocks, &recipients(), 1_000);
        assert_eq!(rewards.len(), 2);
        assert_eq!(rewards[0].amount_sompi, 60);
        assert_eq!(rewards[0].index, 1, "output position is the outpoint index");
        assert_eq!(rewards[1].index, 2);
        assert_eq!(rewards[0].block_daa_score, 100);
        assert_eq!(last, Some(blocks[0].hash));
    }

    #[test]
    fn holds_back_immature_blocks_and_cursor() {
        // Blocks at DAA 100 and 200 are ingestible; 900 is inside the
        // maturity margin and must be left for a later sweep — the cursor
        // stops at 200.
        let blocks = vec![
            block(1, 100, vec![(&TREASURY, 10)]),
            block(2, 200, vec![(&TREASURY, 20)]),
            block(3, 900, vec![(&TREASURY, 30)]),
        ];
        let (rewards, last) = extract_treasury_rewards(&blocks, &recipients(), 500);
        assert_eq!(rewards.len(), 2);
        assert_eq!(last, Some(blocks[1].hash));
    }

    #[test]
    fn nothing_ingestible_keeps_cursor() {
        let blocks = vec![block(1, 900, vec![(&TREASURY, 10)])];
        let (rewards, last) = extract_treasury_rewards(&blocks, &recipients(), 500);
        assert!(rewards.is_empty());
        assert_eq!(last, None, "cursor must not advance past an immature block");
    }

    #[test]
    fn no_treasury_outputs_still_advances_cursor() {
        // A block paying someone else is fully processed — the cursor moves
        // past it even though it yields no rewards.
        let blocks = vec![block(1, 100, vec![(&OTHER, 10)])];
        let (rewards, last) = extract_treasury_rewards(&blocks, &recipients(), 500);
        assert!(rewards.is_empty());
        assert_eq!(last, Some(blocks[0].hash));
    }

    #[test]
    fn maps_txid_bytes_onto_outpoint() {
        let blocks = vec![block(4, 100, vec![(&TREASURY, 10)])];
        let (rewards, _) = extract_treasury_rewards(&blocks, &recipients(), 500);
        assert_eq!(rewards[0].transaction_id, blocks[0].coinbase_txid.as_bytes());
    }
}
