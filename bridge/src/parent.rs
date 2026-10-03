//! Parent-chain transport. Consensus remains entirely in the unmodified nodes.
//! Raw protobuf payout strings avoid teaching ZKas consensus about foreign HRPs.
use anyhow::{Context, Result, bail, ensure};
use kaspa_consensus_core::{auxpow::AuxPow, block::Block};
use kaspa_grpc_core::protowire::{self as pb, kaspad_request::Payload as Request, kaspad_response::Payload as Response};
use kaspa_hashes::Hash;
use kaspa_rpc_core::RpcRawBlock;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tonic::transport::{Channel, Endpoint};

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MiningMode {
    #[default]
    Native,
    Kaspa,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParentConfig {
    pub kind: MiningMode,
    pub endpoint: String,
    pub payout_address: String,
    /// Only permitted when the remote node identifies itself as devnet/simnet.
    #[serde(default)]
    pub allow_unsynced_private: bool,
}

impl ParentConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.kind != MiningMode::Native, "native mode has no parent");
        ensure!(!self.endpoint.trim().is_empty(), "parent endpoint is required");
        validate_address(&self.payout_address)?;
        ensure!(self.accepts_address(&self.payout_address), "parent payout address belongs to a different chain");
        Ok(())
    }
    pub fn accepts_address(&self, address: &str) -> bool {
        let prefix = address.split(':').next().unwrap_or("");
        let configured = self.payout_address.split(':').next().unwrap_or("");
        let family = match self.kind {
            MiningMode::Kaspa => matches!(prefix, "kaspa" | "kaspatest" | "kaspadev" | "kaspasim"),
            MiningMode::Native => false,
        };
        family && prefix == configured
    }
}

pub struct ParentRpc {
    pub config: ParentConfig,
    channel: Channel,
    // A failed parent must not stall every worker for a full RPC timeout.
    retry_after: tokio::sync::Mutex<Option<Instant>>,
}

impl ParentRpc {
    pub fn new(config: ParentConfig) -> Result<Self> {
        config.validate()?;
        let endpoint = config.endpoint.trim_start_matches("grpc://");
        let endpoint = if endpoint.starts_with("http") { endpoint.to_owned() } else { format!("http://{endpoint}") };
        let channel = Endpoint::from_shared(endpoint)?.connect_timeout(Duration::from_secs(2)).tcp_nodelay(true).connect_lazy();
        Ok(Self { config, channel, retry_after: tokio::sync::Mutex::new(None) })
    }

    async fn call(&self, payload: Request) -> Result<Response> {
        let mut client = pb::rpc_client::RpcClient::new(self.channel.clone()).max_decoding_message_size(32 * 1024 * 1024);
        // One request per stream also supports nodes without response IDs.
        let request = pb::KaspadRequest { id: 0, payload: Some(payload) };
        let response = tokio::time::timeout(Duration::from_secs(2), async {
            // Some RPC servers close both directions on request-side EOF. Keep the send
            // side alive until its response arrives instead of a finite iterator.
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.send(request).await.expect("new RPC stream receiver is alive");
            let mut stream = client.message_stream(tokio_stream::wrappers::ReceiverStream::new(rx)).await?.into_inner();
            let response = stream.message().await;
            drop(tx);
            response
        })
        .await
        .context("parent RPC deadline exceeded")??;
        response.and_then(|r| r.payload).context("parent closed RPC without a response")
    }

    pub async fn template(&self, commitment: Hash, payee: Option<&str>) -> Result<Block> {
        // A single miner's invalid payout must not trip the shared RPC backoff.
        if let Some(payee) = payee {
            validate_address(payee)?;
            ensure!(self.config.accepts_address(payee), "worker parent address has wrong network/chain");
        }
        if self.retry_after.lock().await.is_some_and(|until| until > Instant::now()) {
            bail!("parent unavailable; retry backoff active");
        }
        let result = self.template_inner(commitment, payee).await;
        *self.retry_after.lock().await = if result.is_err() { Some(Instant::now() + Duration::from_secs(3)) } else { None };
        result
    }

    async fn template_inner(&self, commitment: Hash, payee: Option<&str>) -> Result<Block> {
        let pay = payee.unwrap_or(&self.config.payout_address);
        validate_address(pay)?;
        ensure!(self.config.accepts_address(pay), "worker parent address has wrong network/chain");
        let extra_data = String::from_utf8(AuxPow::embed_commitment(&[], commitment, &[]))?;
        let Response::GetBlockTemplateResponse(r) = self
            .call(Request::GetBlockTemplateRequest(pb::GetBlockTemplateRequestMessage { pay_address: pay.to_owned(), extra_data }))
            .await?
        else {
            bail!("unexpected parent template response")
        };
        if let Some(error) = r.error {
            bail!("parent template rejected: {}", error.message);
        }
        if !r.is_synced {
            ensure!(self.config.allow_unsynced_private, "parent not synchronized");
            let Response::GetCurrentNetworkResponse(network) =
                self.call(Request::GetCurrentNetworkRequest(pb::GetCurrentNetworkRequestMessage {})).await?
            else {
                bail!("cannot establish parent test network")
            };
            ensure!(network.error.is_none(), "parent network RPC failed");
            let network = network.current_network.to_ascii_lowercase();
            ensure!(
                network == "devnet" || network == "simnet" || network.ends_with("-devnet") || network.ends_with("-simnet"),
                "unsynced mining is only allowed on private devnet/simnet, got {network}"
            );
        }
        let raw = RpcRawBlock::try_from(&r.block.context("parent omitted template block")?)?;
        let block = Block::try_from(raw)?;
        ensure!(crate::merged::committed_h_fc(&block) == Some(commitment), "parent template lost ZKas commitment");
        // Catch transaction/wire incompatibility before any miner works on it.
        ensure!(
            kaspa_consensus_core::merkle::calc_hash_merkle_root(block.transactions.iter()) == block.header.hash_merkle_root,
            "parent transaction Merkle root is not compatible"
        );
        Ok(block)
    }

    pub async fn submit(&self, block: &Block) -> Result<()> {
        let raw: RpcRawBlock = block.into();
        let Response::SubmitBlockResponse(r) = self
            .call(Request::SubmitBlockRequest(pb::SubmitBlockRequestMessage {
                block: Some((&raw).into()),
                allow_non_daa_blocks: false,
            }))
            .await?
        else {
            bail!("unexpected parent submit response")
        };
        if let Some(error) = r.error {
            bail!("{}", error.message);
        }
        ensure!(r.reject_reason == 0, "parent reject reason {}", r.reject_reason);
        Ok(())
    }
}

/// Validate Kaspa-family addresses against their literal network HRP.
/// This affects pool configuration only; ZKas address/consensus code is unchanged.
pub fn validate_address(address: &str) -> Result<()> {
    let (prefix, encoded) = address.split_once(':').context("parent address needs a prefix")?;
    ensure!(matches!(prefix, "kaspa" | "kaspatest" | "kaspadev" | "kaspasim"), "unknown parent prefix");
    const ALPHABET: &[u8] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
    ensure!(encoded.len() >= 10 && encoded.len() <= 128, "invalid address length");
    let values: Vec<u8> = encoded
        .bytes()
        .map(|b| ALPHABET.iter().position(|c| *c == b).map(|i| i as u8).context("invalid address character"))
        .collect::<Result<_>>()?;
    let mut c = 1u64;
    for d in prefix.bytes().map(|b| b & 31).chain([0]).chain(values.iter().copied()) {
        let high = c >> 35;
        c = ((c & 0x07ffffffff) << 5) ^ u64::from(d);
        for (i, generator) in [0x98f2bc8e61, 0x79b76d99e2, 0xf33e5fb3c4, 0xae2eabe2a8, 0x1e4f43e470].iter().enumerate() {
            if high & (1 << i) != 0 {
                c ^= generator;
            }
        }
    }
    ensure!(c == 1, "parent address checksum mismatch");
    let mut bytes = Vec::new();
    let mut acc = 0u32;
    let mut bits = 0;
    for value in &values[..values.len() - 8] {
        acc = ((acc << 5) | u32::from(*value)) & 0xffff;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            bytes.push((acc >> bits) as u8);
        }
    }
    ensure!(bits < 5 && (acc & ((1 << bits) - 1)) == 0, "noncanonical address padding");
    ensure!(
        matches!((bytes.first(), bytes.len()), (Some(0), 33) | (Some(1), 34) | (Some(8), 33)),
        "unsupported parent address version or length"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_literal_checksum_and_network() {
        let valid = "kaspa:qqjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgtturx5zd";
        assert!(validate_address(valid).is_ok());
        assert!(validate_address(&valid.replace("kaspa:", "wrongchain:")).is_err());
        assert!(validate_address("kaspa:garbage").is_err());
        let config = ParentConfig {
            kind: MiningMode::Native,
            endpoint: "127.0.0.1:1".into(),
            payout_address: valid.into(),
            allow_unsynced_private: false,
        };
        assert!(config.validate().is_err());
    }
}
