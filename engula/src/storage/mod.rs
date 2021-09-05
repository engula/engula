mod local_storage;

pub use local_storage::LocalStorage;

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::error::Result;
use crate::format::{Cache, Timestamp};
use crate::memtable::MemTable;

#[derive(Clone)]
pub struct StorageOptions {
    pub max_levels: usize,
    pub block_size: usize,
    pub cache: Option<Arc<dyn Cache>>,
}

impl StorageOptions {
    pub fn default() -> StorageOptions {
        StorageOptions {
            max_levels: 4,
            block_size: 8 * 1024,
            cache: None,
        }
    }
}

pub type StorageVersionSender = watch::Sender<Arc<dyn StorageVersion>>;
pub type StorageVersionReceiver = watch::Receiver<Arc<dyn StorageVersion>>;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn current(&self) -> Arc<dyn StorageVersion>;

    fn current_rx(&self) -> StorageVersionReceiver;

    async fn flush_memtable(&self, mem: Arc<dyn MemTable>) -> Result<Arc<dyn StorageVersion>>;
}

#[async_trait]
pub trait StorageVersion: Send + Sync {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>>;
}
