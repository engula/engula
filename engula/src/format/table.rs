use async_trait::async_trait;

use super::{iterator::Iterator, TableDesc, Timestamp};
use crate::error::Result;

#[async_trait]
pub trait TableReader: Send + Sync {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>>;

    async fn new_iterator(&self) -> Result<Box<dyn Iterator>>;
}

#[async_trait]
pub trait TableBuilder: Send {
    async fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]);

    async fn finish(&mut self) -> Result<TableDesc>;
}
