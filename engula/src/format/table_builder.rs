use async_trait::async_trait;

use crate::common::Timestamp;
use crate::Result;

#[async_trait]
pub trait TableBuilder {
    async fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]);

    async fn finish(&mut self) -> Result<()>;
}
