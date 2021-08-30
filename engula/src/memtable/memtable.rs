use std::iter::Iterator;

use async_trait::async_trait;

use crate::common::Timestamp;

pub type MemItem<'a> = (Timestamp, &'a [u8], &'a [u8]);
pub type MemIter<'a> = dyn Iterator<Item = MemItem<'a>> + Send + Sync + 'a;

#[async_trait]
pub trait MemTable: Send + Sync {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Option<Vec<u8>>;

    async fn put(&self, ts: Timestamp, key: Vec<u8>, value: Vec<u8>);

    async fn snapshot(&self) -> Box<dyn MemSnapshot>;

    fn approximate_size(&self) -> usize;
}

pub trait MemSnapshot: Send + Sync {
    fn iter(&self) -> Box<MemIter>;
}
