//! Read-only deployment check: fetch templates, never hash or submit blocks.
use anyhow::{Result, ensure};
use kaspa_stratum_bridge::{BridgeConfig, KaspaApi, parent::MiningMode};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    ensure!(args.len() == 3, "usage: inspect_mining_templates CONFIG ZKAS_ADDRESS");
    let config = BridgeConfig::from_yaml(&std::fs::read_to_string(&args[1])?)?;
    let mut results = Vec::new();
    for (i, instance) in config.instances.iter().enumerate() {
        let mode = instance.mining_mode.ok_or_else(|| anyhow::anyhow!("explicit mode required"))?;
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let parent = instance.parent.clone();
        let payee = parent.as_ref().map(|p| p.payout_address.clone());
        let api = KaspaApi::new_with_parent(config.global.kaspad_address.clone(), None, rx, None, parent).await?;
        let block = api.get_block_template(&args[2], "inspect", "", 90000 + i as u64, 1, payee, 42).await?;
        let merged = api.merged_fc_target(&block).is_some();
        ensure!(merged == (mode != MiningMode::Native), "{mode:?}: unexpected native fallback or merged work");
        ensure!(block.header.aux_pow.is_none(), "unmined template unexpectedly has an AuxPoW witness");
        results.push(serde_json::json!({
            "mode": mode,
            "merged": merged,
            "header_hash": block.header.hash.to_string(),
            "bits": block.header.bits,
            "zkas_commitment": api.merged_chain_hash(&block).map(|h| h.to_string()),
        }));
    }
    println!("{}", serde_json::to_string_pretty(&results)?);
    Ok(())
}
