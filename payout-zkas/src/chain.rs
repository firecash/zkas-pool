//! Chain reads for shielded-payout confirmation, behind a trait so the
//! engine is unit-testable against an in-memory fake.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use kaspa_grpc_client::GrpcClient;
use kaspa_hashes::Hash as KaspaHash;
use kaspa_rpc_core::api::rpc::RpcApi;

/// Errors from the chain reader.
#[derive(Debug, thiserror::Error)]
pub enum ChainError {
    /// Transport / RPC failure.
    #[error("kaspad: {0}")]
    Transport(String),
    /// The confirmation cursor is no longer a chain block (reorg) — the
    /// caller must re-anchor.
    #[error("confirmation cursor left the selected chain")]
    CursorInvalid,
}

/// Result of one forward walk of the virtual chain.
#[derive(Debug, Default)]
pub struct AcceptanceScan {
    /// `txid → accepting chain block's DAA score` for every watched txid
    /// observed accepted in the walked range.
    pub accepted: HashMap<KaspaHash, u64>,
    /// New cursor (last added chain block), if the chain advanced.
    pub new_cursor: Option<KaspaHash>,
}

/// Minimal chain surface the payout engine needs.
#[async_trait]
pub trait ChainReader: Send + Sync {
    /// Current virtual DAA score.
    async fn virtual_daa_score(&self) -> Result<u64, ChainError>;

    /// Current sink (virtual selected parent) — the confirmation cursor's
    /// initial anchor.
    async fn sink(&self) -> Result<KaspaHash, ChainError>;

    /// Whether `txid` is currently in the mempool.
    async fn in_mempool(&self, txid: KaspaHash) -> Result<bool, ChainError>;

    /// Walk the virtual chain forward from `cursor` (exclusive), reporting
    /// the accepting DAA score of any of `watched` seen accepted.
    async fn accepted_since(
        &self,
        cursor: KaspaHash,
        watched: &HashSet<KaspaHash>,
    ) -> Result<AcceptanceScan, ChainError>;
}

/// Production reader over the shared kaspad gRPC client.
pub struct GrpcChainReader {
    client: Arc<GrpcClient>,
}

impl GrpcChainReader {
    /// Wrap an already-connected client (caller owns the connection).
    #[must_use]
    pub const fn new(client: Arc<GrpcClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ChainReader for GrpcChainReader {
    async fn virtual_daa_score(&self) -> Result<u64, ChainError> {
        self.client
            .get_block_dag_info()
            .await
            .map(|i| i.virtual_daa_score)
            .map_err(|e| ChainError::Transport(e.to_string()))
    }

    async fn sink(&self) -> Result<KaspaHash, ChainError> {
        self.client
            .get_sink()
            .await
            .map(|r| r.sink)
            .map_err(|e| ChainError::Transport(e.to_string()))
    }

    async fn in_mempool(&self, txid: KaspaHash) -> Result<bool, ChainError> {
        match self.client.get_mempool_entry(txid, true, false).await {
            Ok(_) => Ok(true),
            // The gRPC client erases typed errors; the "not found" message is
            // the absent-entry signal, anything else is a real failure.
            Err(e) => {
                let msg = e.to_string();
                if msg.to_ascii_lowercase().contains("not found") {
                    Ok(false)
                } else {
                    Err(ChainError::Transport(msg))
                }
            }
        }
    }

    async fn accepted_since(
        &self,
        cursor: KaspaHash,
        watched: &HashSet<KaspaHash>,
    ) -> Result<AcceptanceScan, ChainError> {
        let resp = self
            .client
            .get_virtual_chain_from_block(cursor, true, None)
            .await
            .map_err(|e| {
                // A cursor kaspad no longer knows as a chain block errors out;
                // classify so the engine re-anchors instead of retrying forever.
                let msg = e.to_string();
                if msg.contains("not found") || msg.contains("chain") {
                    ChainError::CursorInvalid
                } else {
                    ChainError::Transport(msg)
                }
            })?;

        let mut scan = AcceptanceScan {
            accepted: HashMap::new(),
            new_cursor: resp.added_chain_block_hashes.last().copied(),
        };
        if watched.is_empty() {
            return Ok(scan);
        }
        for entry in &resp.accepted_transaction_ids {
            let hits: Vec<KaspaHash> = entry
                .accepted_transaction_ids
                .iter()
                .filter(|id| watched.contains(*id))
                .copied()
                .collect();
            if hits.is_empty() {
                continue;
            }
            // Resolve the accepting block's DAA score once per block.
            let daa = self
                .client
                .get_block(entry.accepting_block_hash, false)
                .await
                .map(|b| b.header.daa_score)
                .map_err(|e| ChainError::Transport(e.to_string()))?;
            for txid in hits {
                scan.accepted.insert(txid, daa);
            }
        }
        Ok(scan)
    }
}
