use super::iterator::*;

use async_trait::async_trait;

use super::Timestamp;
use crate::error::Result;

#[async_trait]
pub trait TableReader {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>>;

    async fn new_iterator(&self) -> Result<Box<dyn Iterator>>;
}

#[async_trait]
pub trait TableBuilder {
    async fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]);

    async fn finish(&mut self) -> Result<usize>;
}
