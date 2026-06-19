// crates/simulator/src/lib.rs

pub mod alloy_db;

use alloy_primitives::{Address, Bytes, I256, U256};
use anyhow::Result;
use revm::bytecode::Bytecode;
use revm::context::result::{ExecutionResult, Output};
use revm::context::TxEnv;
use revm::database::CacheDB;
use revm::primitives::{TxKind, KECCAK_EMPTY};
use revm::state::AccountInfo;
use revm::{Context, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext};
use url::Url;

use crate::alloy_db::AlloyDB;

#[derive(Debug)]
pub struct SimOutput {
    pub gas_used: u64,
    pub eth_balance_change: I256,
}

#[derive(Debug)]
pub struct BundleOutput {
    pub total_gas_used: u64,
    pub victim_reverted: bool,
    pub victim_revert_reason: Option<String>,
    pub attacker_profit: I256,
    pub created_address: Option<Address>,
    pub created_code: Option<Bytes>,
}

#[derive(Clone)]
pub struct EvmSimulator {
    db: CacheDB<AlloyDB>,
}

impl EvmSimulator {
    pub fn new(rpc_url: String) -> Result<Self> {
        let url = Url::parse(&rpc_url)?;
        let alloy_db = AlloyDB::new(url);
        let cache_db = CacheDB::new(alloy_db);

        Ok(Self { db: cache_db })
    }

    pub fn set_code(&mut self, address: Address, code: Bytes) {
        let mut info = self.db.load_account(address).unwrap().info.clone();
        info.code = Some(Bytecode::new_raw(code));
        self.db.insert_account_info(address, info);
    }

    pub fn set_balance(&mut self, address: Address, amount_wei: U256) {
        let mut info = if let Ok(acc) = self.db.load_account(address) {
            acc.info.clone()
        } else {
            AccountInfo {
                balance: amount_wei,
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
            }
        };

        info.balance = amount_wei;
        self.db.insert_account_info(address, info);
    }

    pub fn get_nonce(&mut self, address: Address) -> Result<u64> {
        let acc = self
            .db
            .load_account(address)
            .map_err(|e| anyhow::anyhow!("DB Error: {}", e))?;
        Ok(acc.info.nonce)
    }

    pub fn get_balance(&mut self, address: Address) -> Result<U256> {
        let acc = self
            .db
            .load_account(address)
            .map_err(|e| anyhow::anyhow!("DB Error: {}", e))?;
        Ok(acc.info.balance)
    }

    pub fn simulate_tx(
        &mut self,
        from: Address,
        to: Address,
        value: U256,
        input: Bytes,
    ) -> Result<SimOutput> {
        let pre_balance = self.get_balance(from)?;
        let nonce = self.get_nonce(from)?;

        let mut evm = Context::mainnet().with_db(&mut self.db).build_mainnet();

        let mut tx = TxEnv::default();
        tx.caller = from;
        tx.kind = TxKind::Call(to);
        tx.value = value;
        tx.data = input;
        tx.gas_limit = 500_000;
        tx.gas_price = 20_000_000_000;
        tx.nonce = nonce;

        let result = evm
            .transact_commit(tx)
            .map_err(|e| anyhow::anyhow!("EVM Error: {:?}", e))?;

        match result {
            ExecutionResult::Success { gas_used, .. } => {
                let post_balance = self.get_balance(from)?;
                let diff = I256::from_raw(post_balance) - I256::from_raw(pre_balance);
                Ok(SimOutput {
                    gas_used,
                    eth_balance_change: diff,
                })
            }
            ExecutionResult::Revert { .. } => Err(anyhow::anyhow!("Tx Reverted")),
            ExecutionResult::Halt { .. } => Err(anyhow::anyhow!("Tx Halted")),
        }
    }

    pub fn call_raw(&mut self, tx: TxEnv) -> Result<Bytes> {
        let mut evm = Context::mainnet().with_db(&mut self.db).build_mainnet();

        let result = evm
            .transact(tx)
            .map_err(|e| anyhow::anyhow!("EVM Error: {:?}", e))?;

        match result.result {
            ExecutionResult::Success { output, .. } => match output {
                Output::Call(bytes) => Ok(bytes),
                Output::Create(bytes, _) => Ok(bytes),
            },
            ExecutionResult::Revert { output, .. } => Err(anyhow::anyhow!("Revert: {:?}", output)),
            ExecutionResult::Halt { reason, .. } => Err(anyhow::anyhow!("Halted: {:?}", reason)),
        }
    }

    pub fn simulate_bundle(&mut self, txs: Vec<TxEnv>) -> Result<BundleOutput> {
        if txs.is_empty() {
            return Err(anyhow::anyhow!("Bundle kosong"));
        }

        let attacker_address = txs[0].caller;
        let pre_balance = self.get_balance(attacker_address)?;

        let mut total_gas = 0u64;
        let mut victim_reverted = false;
        let mut revert_reason = None;
        let mut last_created_address = None;
        let mut last_created_code = None;

        for (i, tx) in txs.into_iter().enumerate() {
            let mut evm = Context::mainnet().with_db(&mut self.db).build_mainnet();

            let kind = tx.kind;

            let result = evm
                .transact_commit(tx)
                .map_err(|e| anyhow::anyhow!("EVM Error at index {}: {:?}", i, e))?;

            match result {
                ExecutionResult::Success {
                    gas_used, output, ..
                } => {
                    total_gas += gas_used;
                    println!("TX #{} SUCCESS. Gas: {}", i, gas_used);

                    if let Output::Create(bytes, addr) = output {
                        if let Some(a) = addr {
                            println!("DEPLOYMENT DETECTED! Created Address: {:?}", a);
                            last_created_address = Some(a);
                            last_created_code = Some(bytes);
                        }
                    }

                    if let TxKind::Call(to_addr) = kind {
                        if let Ok(acc) = self.db.load_account(to_addr) {
                            let has_code = acc
                                .info
                                .code
                                .as_ref()
                                .map(|c| !c.is_empty())
                                .unwrap_or(false);
                            if !has_code {
                                if i > 0 {
                                    println!("Warning: Memanggil alamat {:?} tanpa kode.", to_addr);
                                }
                            }
                        }
                    }
                }
                ExecutionResult::Revert { output, .. } => {
                    let hex_output = hex::encode(&output);
                    println!("TX #{} REVERTED! Hex: 0x{}", i, hex_output);

                    if i == 1 {
                        victim_reverted = true;
                        revert_reason = Some(format!("Reverted hex: {}", hex_output));
                    }
                }
                ExecutionResult::Halt { reason, .. } => {
                    println!("TX #{} HALTED! Reason: {:?}", i, reason);
                    if i == 1 {
                        victim_reverted = true;
                        revert_reason = Some(format!("Halted: {:?}", reason));
                    }
                }
            }
        }

        let post_balance = self.get_balance(attacker_address)?;
        let diff = I256::from_raw(post_balance) - I256::from_raw(pre_balance);

        Ok(BundleOutput {
            total_gas_used: total_gas,
            victim_reverted,
            victim_revert_reason: revert_reason,
            attacker_profit: diff,
            created_address: last_created_address,
            created_code: last_created_code,
        })
    }

    pub fn simulate_view_call(
        &mut self,
        caller: Address,
        to: Address,
        calldata: Bytes,
    ) -> Result<(Bytes, u64)> {
        let mut tx = TxEnv::default();
        tx.caller = caller;
        tx.kind = TxKind::Call(to);
        tx.data = calldata;
        tx.gas_limit = 1_000_000;
        tx.gas_price = 1; // Harga dummy untuk simulasi view
        tx.value = U256::ZERO;

        let mut evm = Context::mainnet().with_db(&mut self.db).build_mainnet();

        // Eksekusi murni di RAM
        let result = evm
            .transact(tx)
            .map_err(|e| anyhow::anyhow!("EVM Error: {:?}", e))?;

        match result.result {
            ExecutionResult::Success {
                output, gas_used, ..
            } => match output {
                Output::Call(bytes) => Ok((bytes, gas_used)),
                Output::Create(bytes, _) => Ok((bytes, gas_used)),
            },
            ExecutionResult::Revert { output, .. } => {
                Err(anyhow::anyhow!("Revert Hex: 0x{}", hex::encode(&output)))
            }
            ExecutionResult::Halt { reason, .. } => Err(anyhow::anyhow!("Halted: {:?}", reason)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fork_simulation() {
        // Setup URL RPC (Gunakan Anvil/Geth lokal)
        let rpc_url = "http://127.0.0.1:8545".to_string();

        if let Ok(mut sim) = EvmSimulator::new(rpc_url) {
            let alice = Address::from_str("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266").unwrap();
            let bob = Address::from_str("0x70997970C51812dc3A010C7d01b50e0d17dc79C8").unwrap();

            let amount = U256::from(1_000_000_000_000_000_000u64);

            println!("Simulating transfer...");

            match sim.simulate_transfer(alice, bob, amount) {
                Ok(gas) => println!("Gas Used (Forked): {}", gas),
                Err(e) => panic!("Simulation failed: {}", e),
            }

            let bal = sim.get_balance(bob).unwrap();
            println!("Bob Balance on Fork: {}", bal);

            // Verifikasi sederhana (jika Bob awalnya 0 atau 10000 ETH di Anvil)
            assert!(bal >= amount, "Balance Bob seharusnya bertambah");
        } else {
            println!("Skipping test: No RPC connection found or URL invalid. (Start Anvil first!)");
        }
    }
}
