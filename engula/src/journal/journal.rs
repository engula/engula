use async_trait::async_trait;

use crate::Result;

#[async_trait]
pub trait Journal: Send + Sync {
    async fn append(&self, data: Vec<u8>) -> Result<()>;
}
