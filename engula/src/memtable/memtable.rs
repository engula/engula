use async_trait::async_trait;

use crate::common::Timestamp;

#[async_trait]
pub trait Memtable: Send + Sync {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Option<Vec<u8>>;

    async fn insert(&self, ts: Timestamp, key: Vec<u8>, value: Vec<u8>);

    fn approximate_size(&self) -> usize;
}
