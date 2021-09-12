mod journal_service;
mod local_journal;
mod quorum_journal;

pub use journal_server::JournalServer;
pub use journal_service::JournalService;
pub use local_journal::LocalJournal;
pub use quorum_journal::QuorumJournal;

use async_trait::async_trait;

use crate::error::Result;

tonic::include_proto!("engula.journal");

#[async_trait]
pub trait Journal: Send + Sync {
    async fn append(&self, data: Vec<u8>) -> Result<()>;
}
