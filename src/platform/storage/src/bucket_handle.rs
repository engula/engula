use async_trait::async_trait;

use crate::{error::StorageResult, object_handle::ObjectHandle};

#[async_trait]
pub trait BucketHandle {
    fn object(&self, name: &str) -> Box<dyn ObjectHandle>;

    async fn delete_object(&self, name: &str) -> StorageResult<()>;
}
