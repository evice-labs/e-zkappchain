// crates/common/src/lib.rs

use alloy_primitives::{Address, Bytes, U256};
use serde::{Deserialize, Serialize};

pub mod transaction;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleTx {
    pub signer: Address,
    pub to: Address,
    pub value: U256,
    pub data: Bytes,
    pub gas_limit: u64,
    pub nonce: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MevBundle {
    pub block_number: u64,
    pub txs: Vec<BundleTx>,
    pub timestamp: u64,
}
