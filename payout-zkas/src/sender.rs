//! The shielded spend boundary: one recipient, one Orchard transaction.
//!
//! Production drives the battle-tested `shielded-pay` CLI as a subprocess
//! rather than linking Orchard/Halo 2 into the pool binary: the CLI is the
//! exact spend path already live-verified on mainnet (wallet scan, matured
//! note selection, proof, `submit_transaction`), it keeps the heavy prover
//! dependencies out of this workspace, and its contract is trivially
//! scriptable — the accepted txid on stdout, exit 0; exit 1 otherwise.
//!
//! ## Known limitation (single-operator box)
//!
//! The treasury seed is passed via `--owner-seed-hex`, which is visible in
//! `/proc/<pid>/cmdline` for the lifetime of the proof (seconds). On the
//! current single-user VPS this is accepted; before any multi-user
//! deployment, `shielded-pay` should grow an env/stdin seed input and this
//! sender switched over.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use kaspa_hashes::Hash as KaspaHash;
use tokio::process::Command;
use tracing::{info, warn};

/// Default per-send subprocess budget. A send is a full wallet scan plus an
/// Orchard proof; generous so a cold wallet-scan never gets killed mid-spend.
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// Default public fee left for the miner on each payout transaction (sompi).
/// Matches the `shielded-pay send` default.
pub const DEFAULT_PAYOUT_FEE_SOMPI: u64 = 3_000_000;

/// Errors from a shielded send attempt.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    /// The seed file could not be read or did not contain 64 hex chars.
    #[error("treasury seed unavailable: {0}")]
    Seed(String),

    /// The subprocess could not be spawned.
    #[error("spawn `{bin}`: {message}")]
    Spawn {
        /// Binary we tried to run.
        bin: String,
        /// OS error text.
        message: String,
    },

    /// The subprocess exited non-zero — the CLI's `fatal` path, taken
    /// **before** a node acceptance was printed. NOTE: a narrow ambiguity
    /// exists (the node may have admitted the tx while the acceptance
    /// response was lost); the engine treats every failure as
    /// operator-reconcilable via the in-flight guard rather than retrying
    /// blindly.
    #[error("shielded-pay exited {code:?}: {stderr_tail}")]
    Failed {
        /// Process exit code, if any.
        code: Option<i32>,
        /// Last stderr lines for diagnosis.
        stderr_tail: String,
    },

    /// The subprocess exceeded its budget and was killed. Maximum
    /// ambiguity: the transaction may or may not have been submitted.
    #[error("shielded-pay timed out after {seconds}s (killed); submission state unknown")]
    TimedOut {
        /// The budget that elapsed.
        seconds: u64,
    },

    /// Exit 0 but stdout carried no parsable 64-hex txid — a contract
    /// violation worth loud investigation.
    #[error("shielded-pay succeeded but printed no txid (stdout tail: {stdout_tail})")]
    NoTxid {
        /// Last stdout lines for diagnosis.
        stdout_tail: String,
    },
}

impl SendError {
    /// Whether the submission outcome is genuinely unknown (tx may be in
    /// flight). The engine keeps the in-flight guard latched for these.
    #[must_use]
    pub const fn is_ambiguous(&self) -> bool {
        matches!(self, Self::TimedOut { .. } | Self::NoTxid { .. })
    }
}

/// One shielded payment from the pool treasury.
#[async_trait]
pub trait ShieldedSender: Send + Sync {
    /// Pay `amount_sompi` to the bech32 shielded `to` address. Returns the
    /// node-accepted transaction id.
    async fn send(&self, to: &str, amount_sompi: u64) -> Result<KaspaHash, SendError>;
}

/// Production [`ShieldedSender`]: drives `shielded-pay send`.
pub struct ShieldedPayCli {
    /// Path to the `shielded-pay` binary.
    pub bin: PathBuf,
    /// kaspad gRPC endpoint (`host:port`) handed to `-s`.
    pub rpc_server: String,
    /// File whose (trimmed) content is the 64-hex treasury seed. Read per
    /// send so a key rotation needs no engine restart.
    pub seed_file: PathBuf,
    /// Public fee per payout transaction.
    pub fee_sompi: u64,
    /// `--anchor-depth` override; `None` uses the CLI default (which must
    /// match consensus `shielded_anchor_depth`).
    pub anchor_depth: Option<u64>,
    /// Kill budget per send.
    pub timeout: Duration,
}

impl ShieldedPayCli {
    fn read_seed(&self) -> Result<String, SendError> {
        let raw = std::fs::read_to_string(&self.seed_file)
            .map_err(|e| SendError::Seed(format!("read {}: {e}", self.seed_file.display())))?;
        let seed = raw.trim().to_owned();
        if seed.len() != 64 || !seed.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(SendError::Seed(format!(
                "{} must contain exactly 64 hex chars (got {} chars)",
                self.seed_file.display(),
                seed.len()
            )));
        }
        Ok(seed)
    }
}

/// Extract the accepted txid from the CLI's stdout: the **last** line that
/// parses as a 64-hex hash (`send` prints exactly one, after the log lines).
#[must_use]
pub fn parse_txid(stdout: &str) -> Option<KaspaHash> {
    stdout
        .lines()
        .rev()
        .map(str::trim)
        .find(|l| l.len() == 64 && l.bytes().all(|b| b.is_ascii_hexdigit()))
        .and_then(|l| l.parse::<KaspaHash>().ok())
}

fn tail(s: &str, max_lines: usize) -> String {
    let lines: Vec<&str> = s.lines().rev().take(max_lines).collect();
    lines.into_iter().rev().collect::<Vec<_>>().join(" | ")
}

#[async_trait]
impl ShieldedSender for ShieldedPayCli {
    async fn send(&self, to: &str, amount_sompi: u64) -> Result<KaspaHash, SendError> {
        let seed = self.read_seed()?;

        let mut cmd = Command::new(&self.bin);
        cmd.arg("send")
            .arg("-s")
            .arg(&self.rpc_server)
            .arg("--owner-seed-hex")
            .arg(&seed)
            .arg("--to")
            .arg(to)
            .arg("--amount")
            .arg(amount_sompi.to_string())
            .arg("--fee")
            .arg(self.fee_sompi.to_string());
        if let Some(depth) = self.anchor_depth {
            cmd.arg("--anchor-depth").arg(depth.to_string());
        }
        cmd.kill_on_drop(true)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        info!(to = %katpool_domain::redact::address(to), amount_sompi, "shielded send starting (proof takes a while)");

        let child = cmd.spawn().map_err(|e| SendError::Spawn {
            bin: self.bin.display().to_string(),
            message: e.to_string(),
        })?;

        let output = match tokio::time::timeout(self.timeout, child.wait_with_output()).await {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => {
                return Err(SendError::Spawn {
                    bin: self.bin.display().to_string(),
                    message: format!("wait: {e}"),
                });
            }
            Err(_elapsed) => {
                // kill_on_drop reaps the child when the future is dropped.
                warn!("shielded send timed out; process killed; submission state UNKNOWN");
                return Err(SendError::TimedOut {
                    seconds: self.timeout.as_secs(),
                });
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !output.status.success() {
            return Err(SendError::Failed {
                code: output.status.code(),
                stderr_tail: tail(&stderr, 4),
            });
        }
        match parse_txid(&stdout) {
            Some(txid) => {
                info!(%txid, amount_sompi, "shielded send accepted by node");
                Ok(txid)
            }
            None => Err(SendError::NoTxid {
                stdout_tail: tail(&stdout, 4),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    const TXID: &str = "cc2b1da2c931f4164c03b2066cfb3178303567a161e8a393def62c91e824138a";

    #[test]
    fn parses_txid_from_last_line() {
        let out = format!("log line\nanother\n{TXID}\n");
        assert_eq!(parse_txid(&out).unwrap().to_string(), TXID);
    }

    #[test]
    fn picks_last_hash_when_logs_contain_hashes() {
        let other = "9685f4347b9aa2e100bf489f7979a30746d90823d5bfb62309513b1e23ab2274";
        let out = format!("{other}\nnoise\n{TXID}");
        assert_eq!(parse_txid(&out).unwrap().to_string(), TXID);
    }

    #[test]
    fn no_txid_when_stdout_is_logs_only() {
        assert!(parse_txid("submitting...\ndone-ish\n").is_none());
    }

    #[test]
    fn rejects_wrong_length_hex() {
        assert!(parse_txid(&TXID[..40]).is_none());
    }

    #[test]
    fn ambiguity_classification() {
        assert!(SendError::TimedOut { seconds: 1 }.is_ambiguous());
        assert!(SendError::NoTxid { stdout_tail: String::new() }.is_ambiguous());
        assert!(
            !SendError::Failed { code: Some(1), stderr_tail: String::new() }.is_ambiguous()
        );
        assert!(!SendError::Seed("x".into()).is_ambiguous());
    }

    #[tokio::test]
    async fn seed_file_validation() {
        let dir = tempfile::tempdir().unwrap();
        let seed_path = dir.path().join("seed");
        std::fs::write(&seed_path, "not-hex\n").unwrap();
        let cli = ShieldedPayCli {
            bin: "/nonexistent".into(),
            rpc_server: "127.0.0.1:1".into(),
            seed_file: seed_path,
            fee_sompi: DEFAULT_PAYOUT_FEE_SOMPI,
            anchor_depth: None,
            timeout: Duration::from_secs(1),
        };
        let err = cli.send("zkas:x", 1).await.unwrap_err();
        assert!(matches!(err, SendError::Seed(_)), "{err}");
    }
}
