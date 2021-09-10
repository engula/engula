mod compaction_service;
mod local_compaction;
mod remote_compaction;

pub use compaction_service::CompactionService;
pub use local_compaction::LocalCompaction;
pub use remote_compaction::RemoteCompaction;

use crate::error::Result;

use async_trait::async_trait;

tonic::include_proto!("engula.compaction");

#[async_trait]
pub trait CompactionRuntime: Send + Sync {
    async fn compact(&self, input: CompactionInput) -> Result<CompactionOutput>;
}
