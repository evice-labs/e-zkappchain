use alloy::network::Ethereum;
use alloy::providers::{Provider, RootProvider};
use alloy_primitives::{Address, B256, U256};
use revm::bytecode::Bytecode;
use revm::database::{DBErrorMarker, DatabaseRef};
use revm::primitives::KECCAK_EMPTY;
use revm::state::AccountInfo;
use std::sync::Arc;
use tokio::runtime::Handle;
use url::Url;

#[derive(Debug)]
pub struct AlloyDBError(String);

impl std::fmt::Display for AlloyDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlloyDB Error: {}", self.0)
    }
}

impl std::error::Error for AlloyDBError {}
impl DBErrorMarker for AlloyDBError {}

// Ini memungkinkan konversi otomatis dari anyhow::Error ke AlloyDBError
impl From<anyhow::Error> for AlloyDBError {
    fn from(e: anyhow::Error) -> Self {
        AlloyDBError(e.to_string())
    }
}

type AlloyProvider = RootProvider<Ethereum>;

#[derive(Clone)]
pub struct AlloyDB {
    provider: Arc<AlloyProvider>,
}

impl AlloyDB {
    pub fn new(rpc_url: Url) -> Self {
        let provider = RootProvider::<Ethereum>::new_http(rpc_url);

        Self {
            provider: Arc::new(provider),
        }
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        tokio::task::block_in_place(|| Handle::current().block_on(future))
    }
}

impl DatabaseRef for AlloyDB {
    type Error = AlloyDBError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let f = async {
            let balance = self.provider.get_balance(address).await?;
            let nonce = self.provider.get_transaction_count(address).await?;
            let code = self.provider.get_code_at(address).await?;
            // Memaksa return type menjadi Result<..., anyhow::Error>
            Ok::<_, anyhow::Error>((balance, nonce, code))
        };

        // ? di sini akan memanggil From<anyhow::Error> for AlloyDBError
        let (balance, nonce, code_bytes) = self.block_on(f)?;

        let (code, code_hash) = if code_bytes.is_empty() {
            (None, KECCAK_EMPTY)
        } else {
            let bytecode = Bytecode::new_raw(code_bytes);
            let hash = bytecode.hash_slow();
            (Some(bytecode), hash)
        };

        let info = AccountInfo {
            balance,
            nonce,
            code_hash,
            code,
        };

        Ok(Some(info))
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        Ok(Bytecode::new())
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let f = async {
            // Ambil value, jika error convert ke anyhow::Error via '?'
            let value = self.provider.get_storage_at(address, index).await?;
            Ok::<U256, anyhow::Error>(value)
        };

        let value = self.block_on(f)?;
        Ok(value)
    }

    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        Ok(B256::ZERO)
    }
}
