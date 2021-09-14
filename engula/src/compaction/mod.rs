mod compaction_service;
mod local_compaction;
mod remote_compaction;

pub use compaction_server::CompactionServer;
pub use compaction_service::CompactionService;
pub use local_compaction::LocalCompaction;
pub use proto::*;
pub use remote_compaction::RemoteCompaction;

use crate::{error::Result, format};

use async_trait::async_trait;

mod proto {
    tonic::include_proto!("engula.compaction");
}

#[async_trait]
pub trait CompactionRuntime: Send + Sync {
    async fn compact(&self, input: CompactionInput) -> Result<CompactionOutput>;
}
