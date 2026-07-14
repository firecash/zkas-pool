//! De-risk test for real dual-chain merged mining.
//!
//! The single unverified assumption in the merged design: *can this bridge's
//! (ZKas-fork-typed) gRPC client fetch a block template from the upstream
//! **Kaspa** node, convert it to a `Block`, and does our `FCMM || H_fc` commitment
//! survive verbatim in the real Kaspa coinbase?* Everything downstream (dual-target
//! share check, dual submit) is mechanical once this holds.
//!
//! It is an integration test (links only against the lib, not the bin's unit-test
//! module) and `#[ignore]` because it needs a live, synced Kaspa node:
//!   FIRECASH_KASPA_NODE=127.0.0.1:16210 \
//!   cargo test -p kaspa-stratum-bridge --test merged_derisk -- --ignored --nocapture

use kaspa_addresses::Address;
use kaspa_consensus_core::auxpow::AuxPow;
use kaspa_consensus_core::block::Block;
use kaspa_grpc_client::GrpcClient;
use kaspa_hashes::Hash;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::{notify::mode::NotificationMode, GetBlockTemplateRequest};

#[tokio::test]
#[ignore]
async fn merged_derisk_kaspa_template_carries_commitment() {
    let node = std::env::var("FIRECASH_KASPA_NODE").unwrap_or_else(|_| "127.0.0.1:16210".to_string());
    let pay = std::env::var("FIRECASH_KASPA_PAY")
        .unwrap_or_else(|_| "kaspa:qpr3lpklkdlekuzus2yhnswfaypsgxaj7rfz4h3jzujk66ld5g5xs2p9gxuqg".to_string());

    // A stand-in ZKas block hash to commit to.
    let h_fc = Hash::from_bytes([0x5Au8; 32]);
    // extra_data the miner would embed: MAGIC || H_fc (36 bytes).
    let extra_data = AuxPow::embed_commitment(&[], h_fc, &[]);

    // Same client the bridge uses.
    let grpc = if node.starts_with("grpc://") { node.clone() } else { format!("grpc://{node}") };
    let client = GrpcClient::connect_with_args(
        NotificationMode::Direct,
        grpc,
        None,
        true,
        None,
        false,
        Some(500_000),
        Default::default(),
    )
    .await
    .expect("connect to Kaspa node");

    let pay_addr = Address::try_from(pay.as_str()).expect("valid kaspa: pay address");

    // Fetch a real Kaspa template paying our address, carrying our commitment.
    let resp = client
        .get_block_template_call(None, GetBlockTemplateRequest::new(pay_addr, extra_data.clone()))
        .await
        .expect("getBlockTemplate from Kaspa node");

    // The exact conversion the bridge relies on.
    let block = Block::try_from(resp.block).expect("RpcRawBlock -> Block conversion");

    // The parent must be a real Kaspa block: it has a coinbase and a real target.
    assert!(!block.transactions.is_empty(), "template has no transactions");
    assert_ne!(block.header.bits, 0, "template carries a real Kaspa target");

    // Does our commitment survive in the real Kaspa coinbase?
    let recovered = kaspa_stratum_bridge::merged::committed_h_fc(&block)
        .expect("FCMM||H_fc commitment must be present in the Kaspa coinbase payload");
    assert_eq!(recovered, h_fc, "recovered commitment must equal H_fc we asked kaspad to embed");

    // The aux binding half (commitment + coinbase Merkle inclusion) must verify against
    // the real parent — the same check ZKas consensus performs — for single-tx
    // templates (empty branch). Multi-tx would need the real branch; the de-risk only
    // needs the commitment to survive + convert.
    if block.transactions.len() == 1 {
        let coinbase = block.transactions[0].clone();
        let aux = AuxPow { parent_header: (*block.header).clone(), parent_coinbase: coinbase, coinbase_merkle_branch: vec![] };
        assert!(aux.verify_binding(h_fc), "single-tx parent must verify its aux binding");
    }

    println!(
        "DERISK OK: kaspa template @ blue_score={} bits=0x{:08x} txs={} commitment survives + converts",
        block.header.blue_score,
        block.header.bits,
        block.transactions.len()
    );
}
