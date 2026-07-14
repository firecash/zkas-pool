//! Merged-mining (AuxPoW) support for the stratum bridge — the pool-side engine that
//! lets ASICs merge-mine ZKas.
//!
//! It plugs in as a decorator around [`crate::kaspaapi::KaspaApi`] so the entire
//! stratum/job/share-validation core stays untouched:
//!
//! * **get_block_template** — fetch a ZKas template, then hand the ASIC a
//!   *parent* (Kaspa-shaped) block whose single coinbase commits to the ZKas
//!   block hash `H_fc` and whose target (`bits`) is the ZKas target. The ASIC
//!   grinds the parent's kHeavyHash exactly as it would a normal job.
//! * **submit_block** — when a share clears the target, the "block" the bridge hands
//!   back is that solved parent. We rebuild the [`AuxPow`] from it and submit the
//!   *ZKas* block carrying the aux proof (the node accepts it via Option-2 dual
//!   acceptance). Because the aux rides `RpcRawHeader.aux_pow`, the existing
//!   `(&block).into()` submit path transmits it unchanged.
//!
//! In this first version the parent is synthetic (self-generated), which proves the
//! ASIC→aux path end to end. Swapping the synthetic parent for a live Kaspa
//! `getBlockTemplate` (and also submitting winning parents to Kaspa) is what adds the
//! second-chain reward — the struct is identical, only the parent's source changes.

use std::collections::{HashMap, VecDeque};

use kaspa_consensus_core::{
    auxpow::AuxPow, block::Block, hashing, header::Header, merkle::calc_hash_merkle_root, subnets::SUBNETWORK_ID_COINBASE,
    tx::Transaction,
};
use kaspa_hashes::{Hash, ZERO_HASH};

/// Build the coinbase (leaf 0) Merkle inclusion branch for a *real* multi-tx parent,
/// i.e. the sequence of right-sibling hashes from the coinbase up to the parent's
/// `hash_merkle_root`. Reproduces Kaspa's tx Merkle tree
/// ([`kaspa_consensus_core::merkle::calc_hash_merkle_root`]): a full binary tree padded
/// to the next power of two, where a wholly-absent right subtree folds against
/// `ZERO_HASH` (matching `calc_merkle_root_with_hasher`'s `unwrap_or(ZERO_HASH)`).
///
/// The coinbase is always leaf 0, hence the left child at every level, so the branch is
/// exactly what [`AuxPow::verify_coinbase_inclusion`] folds (`acc = merkle_hash(acc,
/// sibling)`). Returns an empty branch for a single-tx parent (coinbase is the root).
pub fn coinbase_merkle_branch(txs: &[Transaction]) -> Vec<Hash> {
    if txs.len() <= 1 {
        return vec![];
    }
    // Leaves = per-tx hashes, padded to a power of two with `None` (absent leaves).
    let mut level: Vec<Option<Hash>> = txs.iter().map(|t| Some(hashing::tx::hash(t))).collect();
    level.resize(level.len().next_power_of_two(), None);

    let mut branch = Vec::new();
    let mut idx = 0usize; // coinbase index; stays 0 (leftmost) all the way up
    while level.len() > 1 {
        // Right sibling of the coinbase path. An absent subtree contributes ZERO_HASH,
        // exactly as consensus folds it.
        branch.push(level[idx ^ 1].unwrap_or(ZERO_HASH));
        let mut next = Vec::with_capacity(level.len() / 2);
        for pair in level.chunks(2) {
            let combined = match pair[0] {
                None => None,
                Some(l) => Some(kaspa_merkle::merkle_hash(l, pair.get(1).copied().flatten().unwrap_or(ZERO_HASH))),
            };
            next.push(combined);
        }
        idx /= 2;
        level = next;
    }
    branch
}

/// Build the parent block an ASIC hashes in merged mode: one coinbase committing to
/// `H_fc`, with the ZKas target. Returns `(parent_block, h_fc)`.
pub fn build_parent_block(fc_block: &Block) -> (Block, Hash) {
    let h_fc = fc_block.header.hash;
    let coinbase = Transaction::new(0, vec![], vec![], 0, SUBNETWORK_ID_COINBASE, 0, AuxPow::embed_commitment(&[], h_fc, &[]));
    let hash_merkle_root = calc_hash_merkle_root(std::iter::once(&coinbase));

    let mut parent = Header::from_precomputed_hash(ZERO_HASH, vec![Hash::from_u64_word(0xF12E_CA54)]);
    parent.hash_merkle_root = hash_merkle_root;
    parent.bits = fc_block.header.bits; // ASIC grinds against the ZKas target
    parent.timestamp = fc_block.header.timestamp;
    parent.finalize();

    (Block::new(parent, vec![coinbase]), h_fc)
}

/// The `H_fc` a parent block commits to (from its coinbase), or `None` if it carries
/// no valid commitment (i.e. this wasn't a merged-mining parent).
pub fn committed_h_fc(parent_block: &Block) -> Option<Hash> {
    let coinbase = parent_block.transactions.first()?.clone();
    AuxPow { parent_header: (*parent_block.header).clone(), parent_coinbase: coinbase, coinbase_merkle_branch: vec![] }.committed_hash()
}

/// Assemble the ZKas block carrying the AuxPoW proof, from the solved `parent_block`
/// (the bridge has set the winning nonce on its header) and the stashed `fc_block`.
///
/// Builds the *real* coinbase Merkle branch from the parent's transactions, so this is
/// correct for real multi-tx Kaspa parents (not just the single-tx synthetic case).
pub fn assemble_aux_block(parent_block: &Block, fc_block: &Block) -> Block {
    let coinbase = parent_block.transactions[0].clone();
    let branch = coinbase_merkle_branch(&parent_block.transactions);
    let aux = AuxPow { parent_header: (*parent_block.header).clone(), parent_coinbase: coinbase, coinbase_merkle_branch: branch };
    let fc_header = (*fc_block.header).clone().with_aux_pow(aux);
    Block::new(fc_header, (*fc_block.transactions).clone())
}

/// A small bounded map from `H_fc` to the ZKas block awaiting a solved parent.
/// Bounded FIFO so a busy pool that never solves a given template doesn't grow it
/// without limit.
pub struct MergedPending {
    map: HashMap<Hash, Block>,
    order: VecDeque<Hash>,
    cap: usize,
}

impl MergedPending {
    pub fn new(cap: usize) -> Self {
        Self { map: HashMap::new(), order: VecDeque::new(), cap: cap.max(1) }
    }

    pub fn insert(&mut self, h_fc: Hash, fc_block: Block) {
        if self.map.insert(h_fc, fc_block).is_none() {
            self.order.push_back(h_fc);
            while self.order.len() > self.cap {
                if let Some(old) = self.order.pop_front() {
                    self.map.remove(&old);
                }
            }
        }
    }

    pub fn get(&self, h_fc: &Hash) -> Option<Block> {
        self.map.get(h_fc).cloned()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    fn coinbase(h_fc: Hash) -> Transaction {
        Transaction::new(0, vec![], vec![], 0, SUBNETWORK_ID_COINBASE, 0, AuxPow::embed_commitment(&[1, 2, 3], h_fc, &[9]))
    }
    fn tx(tag: u8) -> Transaction {
        use kaspa_consensus_core::subnets::SUBNETWORK_ID_NATIVE;
        Transaction::new(0, vec![], vec![], 0, SUBNETWORK_ID_NATIVE, 0, vec![tag; 16])
    }

    /// For every parent size 1..=9, the branch we build must fold the coinbase back to
    /// the canonical `calc_hash_merkle_root`, i.e. `verify_coinbase_inclusion` passes —
    /// proving the branch matches Kaspa's real tx Merkle tree, not just the 1-tx case.
    #[test]
    fn coinbase_branch_matches_real_merkle_root_for_all_sizes() {
        let h_fc = Hash::from_bytes([0x5Au8; 32]);
        for n in 1..=9usize {
            let mut txs = vec![coinbase(h_fc)];
            for i in 1..n {
                txs.push(tx(i as u8));
            }
            let root = calc_hash_merkle_root(txs.iter());
            let mut header = Header::from_precomputed_hash(ZERO_HASH, vec![]);
            header.hash_merkle_root = root;
            let aux = AuxPow {
                parent_header: header,
                parent_coinbase: txs[0].clone(),
                coinbase_merkle_branch: coinbase_merkle_branch(&txs),
            };
            assert!(aux.verify_coinbase_inclusion(), "branch must reproduce the root for n={n} txs");
            assert!(aux.verify_binding(h_fc), "full binding (commitment + inclusion) must hold for n={n}");
        }
    }
}
