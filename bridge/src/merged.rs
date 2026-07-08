//! Merged-mining (AuxPoW) support for the stratum bridge — the pool-side engine that
//! lets ASICs merge-mine FireCash.
//!
//! It plugs in as a decorator around [`crate::kaspaapi::KaspaApi`] so the entire
//! stratum/job/share-validation core stays untouched:
//!
//! * **get_block_template** — fetch a FireCash template, then hand the ASIC a
//!   *parent* (Kaspa-shaped) block whose single coinbase commits to the FireCash
//!   block hash `H_fc` and whose target (`bits`) is the FireCash target. The ASIC
//!   grinds the parent's kHeavyHash exactly as it would a normal job.
//! * **submit_block** — when a share clears the target, the "block" the bridge hands
//!   back is that solved parent. We rebuild the [`AuxPow`] from it and submit the
//!   *FireCash* block carrying the aux proof (the node accepts it via Option-2 dual
//!   acceptance). Because the aux rides `RpcRawHeader.aux_pow`, the existing
//!   `(&block).into()` submit path transmits it unchanged.
//!
//! In this first version the parent is synthetic (self-generated), which proves the
//! ASIC→aux path end to end. Swapping the synthetic parent for a live Kaspa
//! `getBlockTemplate` (and also submitting winning parents to Kaspa) is what adds the
//! second-chain reward — the struct is identical, only the parent's source changes.

use std::collections::{HashMap, VecDeque};

use kaspa_consensus_core::{
    auxpow::AuxPow, block::Block, header::Header, merkle::calc_hash_merkle_root, subnets::SUBNETWORK_ID_COINBASE, tx::Transaction,
};
use kaspa_hashes::{Hash, ZERO_HASH};

/// Build the parent block an ASIC hashes in merged mode: one coinbase committing to
/// `H_fc`, with the FireCash target. Returns `(parent_block, h_fc)`.
pub fn build_parent_block(fc_block: &Block) -> (Block, Hash) {
    let h_fc = fc_block.header.hash;
    let coinbase = Transaction::new(0, vec![], vec![], 0, SUBNETWORK_ID_COINBASE, 0, AuxPow::embed_commitment(&[], h_fc, &[]));
    let hash_merkle_root = calc_hash_merkle_root(std::iter::once(&coinbase));

    let mut parent = Header::from_precomputed_hash(ZERO_HASH, vec![Hash::from_u64_word(0xF12E_CA54)]);
    parent.hash_merkle_root = hash_merkle_root;
    parent.bits = fc_block.header.bits; // ASIC grinds against the FireCash target
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

/// Assemble the FireCash block carrying the AuxPoW proof, from the solved `parent_block`
/// (the bridge has set the winning nonce on its header) and the stashed `fc_block`.
pub fn assemble_aux_block(parent_block: &Block, fc_block: &Block) -> Block {
    let coinbase = parent_block.transactions[0].clone();
    let aux = AuxPow { parent_header: (*parent_block.header).clone(), parent_coinbase: coinbase, coinbase_merkle_branch: vec![] };
    let fc_header = (*fc_block.header).clone().with_aux_pow(aux);
    Block::new(fc_header, (*fc_block.transactions).clone())
}

/// A small bounded map from `H_fc` to the FireCash block awaiting a solved parent.
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
