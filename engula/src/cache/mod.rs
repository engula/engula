use std::sync::Arc;

use async_trait::async_trait;

#[async_trait]
pub trait Cache: Send + Sync {
    async fn get(&self, key: &[u8]) -> Option<Arc<Vec<u8>>>;

    async fn put(&self, key: Vec<u8>, value: Vec<u8>);
}
