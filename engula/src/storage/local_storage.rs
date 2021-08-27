use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::watch;

use super::storage::*;
use crate::common::Timestamp;
use crate::Result;

pub struct LocalStorage {
    current: StorageVersionRef,
    current_tx: StorageVersionSender,
    current_rx: StorageVersionReceiver,
}

impl LocalStorage {
    pub fn new() -> LocalStorage {
        let current: StorageVersionRef = Arc::new(Box::new(LocalStorageVersion::new()));
        let (tx, rx) = watch::channel(current.clone());
        LocalStorage {
            current,
            current_tx: tx,
            current_rx: rx,
        }
    }
}

impl Storage for LocalStorage {
    fn current(&self) -> StorageVersionRef {
        self.current.clone()
    }

    fn current_rx(&self) -> StorageVersionReceiver {
        self.current_rx.clone()
    }
}

pub struct LocalStorageVersion {}

impl LocalStorageVersion {
    fn new() -> LocalStorageVersion {
        LocalStorageVersion {}
    }
}

#[async_trait]
impl StorageVersion for LocalStorageVersion {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}
