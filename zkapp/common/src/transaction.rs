use borsh::{BorshDeserialize, BorshSerialize};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::cmp::Ordering;
use sha2::Digest;

use sequencer_core::{Address, FullPublicKey, Signature, VrfPublicKeyBytes};

#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Serialize,
    Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Encode,
    Decode,
)]
pub enum TransactionData {
    Transfer {
        recipient: Address,
        amount: u64,
    },
    Stake {
        amount: u64,
    },
    SubmitRollupBatch {
        #[serde(with = "serde_bytes")]
        old_state_root: Vec<u8>,
        #[serde(with = "serde_bytes")]
        new_state_root: Vec<u8>,
        #[serde(with = "serde_bytes")]
        compressed_batch: Vec<u8>,
        #[serde(with = "serde_bytes")]
        zk_proof: Vec<u8>,
        #[serde(default)]
        is_test_tx: bool,
        #[serde(with = "serde_bytes")]
        vrf_output: Vec<u8>,
        #[serde(with = "serde_bytes")]
        vrf_proof: Vec<u8>,
        dac_signatures: Vec<Vec<u8>>,
    },
    SubmitAggregateRollupBatch {
        #[serde(with = "serde_bytes")]
        initial_state_root: Vec<u8>,
        #[serde(with = "serde_bytes")]
        final_state_root: Vec<u8>,
        #[serde(with = "serde_bytes")]
        aggregated_proof: Vec<u8>,
        num_batches: u32,
    },
    DepositToL2 {
        amount: u64,
    },
    UpdateVrfKey {
        #[serde(with = "serde_bytes")]
        new_vrf_public_key: VrfPublicKeyBytes,
    },
    RegisterAsSequencer,
    DeregisterAsSequencer,
    DeployContract {
        #[serde(with = "serde_bytes")]
        code: Vec<u8>,
    },
    CallContract {
        contract_address: Address,
        #[serde(with = "serde_bytes")]
        call_data: Vec<u8>,
    },
    UpdateNetworkIdentity {
        #[serde(with = "serde_bytes")]
        multiaddr: Vec<u8>,
    },
    ImAlive,
}

impl TransactionData {
    pub fn base_gas_cost(&self) -> u64 {
        const BASE_TX_GAS: u64 = 21_000;
        match self {
            TransactionData::Transfer { .. } => BASE_TX_GAS,
            TransactionData::Stake { .. } => BASE_TX_GAS + 5_000,
            TransactionData::DepositToL2 { .. } => BASE_TX_GAS + 20_000,
            TransactionData::SubmitRollupBatch {
                compressed_batch, ..
            } => BASE_TX_GAS + 300_000 + (compressed_batch.len() as u64 * 50),
            TransactionData::SubmitAggregateRollupBatch { num_batches, .. } => {
                BASE_TX_GAS + 500_000 + (u64::from(*num_batches) * 10_000)
            }
            TransactionData::DeployContract { code } => {
                BASE_TX_GAS + 150_000 + (code.len() as u64 * 200)
            }
            TransactionData::CallContract { .. } => BASE_TX_GAS + 5_000,
            TransactionData::UpdateVrfKey { .. } => BASE_TX_GAS + 7_000,
            TransactionData::RegisterAsSequencer | TransactionData::DeregisterAsSequencer => {
                BASE_TX_GAS + 10_000
            }
            TransactionData::UpdateNetworkIdentity { multiaddr } => {
                BASE_TX_GAS + 1_000 + (multiaddr.len() as u64 * 20)
            }
            TransactionData::ImAlive => BASE_TX_GAS,
        }
    }
}

#[derive(
    BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone, Eq, Encode, Decode,
)]
pub struct Transaction {
    pub sender_public_key: FullPublicKey,
    pub data: TransactionData,
    pub nonce: u64,
    pub max_fee_per_gas: u64,
    pub max_priority_fee_per_gas: u64,
    #[serde(with = "serde_bytes")]
    pub signature: Signature,
    pub chain_id: String,
}

impl PartialEq for Transaction {
    fn eq(&self, other: &Self) -> bool {
        self.message_hash() == other.message_hash()
    }
}

impl Ord for Transaction {
    fn cmp(&self, other: &Self) -> Ordering {
        const FAKE_BASE_FEE: u64 = 10;
        let self_effective_tip = self
            .max_fee_per_gas
            .saturating_sub(FAKE_BASE_FEE)
            .min(self.max_priority_fee_per_gas);
        let other_effective_tip = other
            .max_fee_per_gas
            .saturating_sub(FAKE_BASE_FEE)
            .min(other.max_priority_fee_per_gas);
        self_effective_tip
            .cmp(&other_effective_tip)
            .then_with(|| other.nonce.cmp(&self.nonce))
    }
}

impl PartialOrd for Transaction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for Transaction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.message_hash().hash(state);
    }
}

impl Transaction {
    pub fn sender(&self) -> Address {
        sequencer_core::crypto::public_key_to_address(&self.sender_public_key.0)
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(self.chain_id.as_bytes());
        data.extend_from_slice(self.sender_public_key.as_ref());
        data.extend_from_slice(
            &bincode::encode_to_vec(&self.data, bincode::config::standard()).unwrap(),
        );
        data.extend_from_slice(&self.nonce.to_be_bytes());
        data.extend_from_slice(&self.max_fee_per_gas.to_be_bytes());
        data.extend_from_slice(&self.max_priority_fee_per_gas.to_be_bytes());
        data
    }

    pub fn message_hash(&self) -> Vec<u8> {
        let data = self.canonical_bytes();
        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, data);
        sha2::Digest::finalize(hasher).to_vec()
    }
}
