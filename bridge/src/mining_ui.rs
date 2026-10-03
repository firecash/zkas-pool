//! Public native/Kaspa connection metadata. No RPC endpoints or payout defaults.
use crate::{BridgeConfig, parent::MiningMode};
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MiningConnection {
    pub instance: String,
    pub mode: MiningMode,
    pub port: u16,
    pub parent_prefix: Option<String>,
}

pub fn connections(config: &BridgeConfig) -> Vec<MiningConnection> {
    config
        .instances
        .iter()
        .enumerate()
        .filter_map(|(i, instance)| {
            let mode = instance.mining_mode?;
            let port = instance.stratum_port.rsplit(':').next()?.parse::<u16>().ok()?;
            if port == 0 {
                return None;
            }
            let parent_prefix = match mode {
                MiningMode::Native => None,
                MiningMode::Kaspa => {
                    let pay = instance.parent.as_ref().map(|p| p.payout_address.clone()).unwrap_or_else(|| {
                        std::env::var("ZKAS_KASPA_PAY")
                            .or_else(|_| std::env::var("FIRECASH_KASPA_PAY"))
                            .unwrap_or_else(|_| config.global.merged_kaspa_pay_address.clone())
                    });
                    let address = kaspa_addresses::Address::try_from(pay.as_str()).ok()?;
                    let prefix = address.prefix.to_string();
                    if !matches!(prefix.as_str(), "kaspa" | "kaspatest" | "kaspadev" | "kaspasim") {
                        return None;
                    }
                    Some(prefix)
                }
            };
            Some(MiningConnection { instance: crate::log_colors::LogColors::format_instance_id(i + 1), mode, port, parent_prefix })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn publishes_native_and_kaspa_without_private_configuration() {
        let mut config = BridgeConfig::default();
        assert!(connections(&config).is_empty());
        config.instances[0].mining_mode = Some(MiningMode::Native);
        config.instances[0].stratum_port = "127.0.0.1:5555".into();
        let mut merged = config.instances[0].clone();
        merged.mining_mode = Some(MiningMode::Kaspa);
        merged.stratum_port = "127.0.0.1:5556".into();
        config.instances.push(merged);
        config.global.merged_kaspa_pay_address = "kaspa:qqjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgtturx5zd".into();
        let options = connections(&config);
        assert_eq!(options.len(), 2);
        assert_eq!(options[0].port, 5555);
        assert!(options[0].parent_prefix.is_none());
        assert_eq!(options[1].parent_prefix.as_deref(), Some("kaspa"));
        let json = serde_json::to_string(&options).unwrap();
        assert!(!json.contains("endpoint"));
        assert!(!json.contains("payout_address"));
        assert!(!json.contains(&config.global.merged_kaspa_pay_address));
    }
}
