use async_trait::async_trait;

use crate::{bucket_handle::BucketHandle, error::StorageResult};

#[async_trait]
pub trait ObjectStorage {
    fn bucket(&self, name: &str) -> Box<dyn BucketHandle>;

    async fn create_bucket(&self, name: &str) -> StorageResult<()>;

    async fn delete_bucket(&self, name: &str) -> StorageResult<()>;
}
