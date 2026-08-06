//! Deterministic, chain-free tests for the kasplex KRC-20 inscription
//! primitives. These pin the exact on-chain envelope bytes so any drift
//! from the kasplex-accepted production format fails loudly.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use kaspa_addresses::{Prefix, Version};
use kaspa_txscript::script_builder::ScriptBuilder;
use payout_krc20::{
    Krc20Transfer, build_transfer_inscription, commit_address, commit_script_public_key,
    reveal_signature_script,
};

/// A fixed, non-secret 32-byte x-only public key for reproducible vectors.
const XONLY_PK: [u8; 32] = [
    0x1b, 0x91, 0x5b, 0x4c, 0x2a, 0x77, 0x0e, 0x3f, 0x44, 0x09, 0xd8, 0x60, 0xb1, 0x22, 0x6e, 0x90,
    0xc5, 0x33, 0xaa, 0x18, 0x7d, 0x6f, 0x4e, 0x21, 0x88, 0x99, 0x10, 0x55, 0xe7, 0x3c, 0xba, 0x02,
];

const RECIPIENT: &str = "kaspatest:qqkq3vz9j8m8k0r2c8x4n5p6w7s9t0u1v2x3y4z5a6b7c8d9e0f";

fn sample_transfer() -> Krc20Transfer {
    Krc20Transfer::new("NACHO", "100000000", RECIPIENT)
}

#[test]
fn transfer_json_is_canonical_compact_kasplex() {
    let json = sample_transfer().to_json().expect("serialise transfer");
    let expected = format!(
        r#"{{"p":"krc-20","op":"transfer","tick":"NACHO","amt":"100000000","to":"{RECIPIENT}"}}"#
    );
    assert_eq!(
        json, expected,
        "field order/compactness must match the indexer + production pool"
    );
}

#[test]
fn envelope_is_byte_exact_kasplex_layout() {
    let transfer = sample_transfer();
    let script = build_transfer_inscription(&XONLY_PK, &transfer).expect("build inscription");

    // Reconstruct the envelope independently from first principles:
    //   <0x20 pk> OP_CHECKSIG OP_FALSE OP_IF <0x07 "kasplex"> OP_0 <json> OP_ENDIF
    let json = transfer.to_json().expect("serialise transfer");
    let json_push = ScriptBuilder::new()
        .add_data(json.as_bytes())
        .expect("json push")
        .drain();
    let mut expected = Vec::new();
    expected.push(0x20); // push 32 bytes (OpData32)
    expected.extend_from_slice(&XONLY_PK);
    expected.push(0xac); // OP_CHECKSIG (Schnorr, not 0xab ECDSA)
    expected.push(0x00); // OP_FALSE
    expected.push(0x63); // OP_IF
    expected.push(0x07); // push 7 bytes
    expected.extend_from_slice(b"kasplex");
    expected.push(0x00); // OP_0 (add_i64(0)) — single push, not OP_1 OP_0 OP_0
    expected.extend_from_slice(&json_push);
    expected.push(0x68); // OP_ENDIF

    assert_eq!(
        script, expected,
        "redeem script must be byte-identical to the kasplex envelope"
    );
}

#[test]
fn commit_address_is_testnet_p2sh_and_binds_the_data() {
    let script = build_transfer_inscription(&XONLY_PK, &sample_transfer()).expect("build");
    let addr = commit_address(&script, Prefix::Testnet).expect("p2sh address");

    assert_eq!(
        addr.version,
        Version::ScriptHash,
        "commit output must be P2SH"
    );
    assert_eq!(addr.prefix, Prefix::Testnet);
    assert!(
        addr.to_string().starts_with("zkastest:"),
        "testnet-10 prefix"
    );

    // The P2SH script public key is blake2b-256(redeem_script): changing the
    // recipient must change the address (the hash binds the inscription).
    let other = build_transfer_inscription(
        &XONLY_PK,
        &Krc20Transfer::new(
            "NACHO",
            "100000000",
            "kaspatest:qqdifferentrecipientaddresshere00",
        ),
    )
    .expect("build other");
    let other_addr = commit_address(&other, Prefix::Testnet).expect("other p2sh");
    assert_ne!(addr, other_addr, "redeem-script hash must bind the payload");
}

#[test]
fn commit_script_public_key_is_well_formed_p2sh() {
    let script = build_transfer_inscription(&XONLY_PK, &sample_transfer()).expect("build");
    let spk = commit_script_public_key(&script);
    // Standard P2SH spk: OP_BLAKE2B <0x20 hash..32> OP_EQUAL  → 35 bytes.
    let bytes = spk.script();
    assert_eq!(
        bytes.len(),
        35,
        "P2SH spk is opcode + 32-byte-push + opcode"
    );
    assert_eq!(bytes[1], 0x20, "32-byte hash push");
}

#[test]
fn reveal_signature_script_is_signature_then_pushed_redeem() {
    let redeem = build_transfer_inscription(&XONLY_PK, &sample_transfer()).expect("build");
    let signature = vec![0x41u8; 65]; // dummy Schnorr-sized sig + sighash byte
    let sig_script =
        reveal_signature_script(redeem.clone(), signature.clone()).expect("reveal sig script");

    let expected_redeem_push = ScriptBuilder::new()
        .add_data(&redeem)
        .expect("push")
        .drain();
    let mut expected = signature.clone();
    expected.extend_from_slice(&expected_redeem_push);

    assert_eq!(
        sig_script, expected,
        "reveal sig script must be <sig><pushed redeem script>"
    );
    assert!(sig_script.starts_with(&signature), "signature comes first");
}
