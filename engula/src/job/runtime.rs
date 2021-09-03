use async_trait::async_trait;

use super::{JobInput, JobOutput};
use crate::error::Result;

#[async_trait]
pub trait JobRuntime: Send + Sync {
    async fn spawn(&self, input: JobInput) -> Result<JobOutput>;
}
