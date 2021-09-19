mod journal_service;
mod local_journal;
mod remote_journal;
mod write;

pub use journal_server::JournalServer;
pub use journal_service::JournalService;
pub use local_journal::LocalJournal;
pub use proto::*;
pub use remote_journal::RemoteJournal;
pub use write::{Write, WriteBatch};

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::error::Result;

mod proto {
    tonic::include_proto!("engula.journal");
}

#[derive(Clone, Debug)]
pub struct JournalOptions {
    pub sync: bool,
    pub size: usize,
}

impl JournalOptions {
    pub fn default() -> JournalOptions {
        JournalOptions {
            sync: true,
            size: 1024 * 1024,
        }
    }
}

#[async_trait]
pub trait Journal: Send + Sync {
    async fn append_stream(&self, rx: mpsc::Receiver<WriteBatch>) -> Result<()>;
}
