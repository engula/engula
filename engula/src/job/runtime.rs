use async_trait::async_trait;

use crate::error::Result;
use crate::job::{JobInput, JobOutput};

#[async_trait]
pub trait JobRuntime: Send + Sync {
    async fn spawn(&self, input: JobInput) -> Result<JobOutput>;
}
