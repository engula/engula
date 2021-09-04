mod local_journal;
mod quorum_journal;
mod service;

pub use local_journal::LocalJournal;
pub use quorum_journal::QuorumJournal;
pub use service::Service as JournalService;

use async_trait::async_trait;

use crate::error::Result;

tonic::include_proto!("engula.journal");

#[async_trait]
pub trait Journal: Send + Sync {
    async fn append(&self, data: Vec<u8>) -> Result<()>;
}
