mod journal_service;
mod local_journal;
mod quorum_journal;
mod write;

pub use journal_server::JournalServer;
pub use journal_service::JournalService;
pub use local_journal::LocalJournal;
pub use proto::*;
pub use quorum_journal::QuorumJournal;
pub use write::{Write, WriteBatch};

use async_trait::async_trait;
use tokio::sync::mpsc;
use url::Url;

use crate::error::{Error, Result};

mod proto {
    tonic::include_proto!("engula.journal");
}

#[derive(Clone, Debug)]
pub struct JournalOptions {
    pub sync: bool,
    pub chunk_size: usize,
}

impl JournalOptions {
    pub fn default() -> JournalOptions {
        JournalOptions {
            sync: false,
            chunk_size: 1024,
        }
    }
}

#[async_trait]
pub trait Journal: Send + Sync {
    async fn append_stream(&self, rx: mpsc::Receiver<WriteBatch>) -> Result<()>;
}

pub async fn open_journal(url: &str, options: JournalOptions) -> Result<Box<dyn Journal>> {
    let parsed_url = Url::parse(url)?;
    match parsed_url.scheme() {
        "file" => {
            let journal = LocalJournal::new(parsed_url.path(), options)?;
            Ok(Box::new(journal))
        }
        "http" => {
            let urls = vec![url.to_owned()];
            let journal = QuorumJournal::new(urls, options).await?;
            Ok(Box::new(journal))
        }
        _ => Err(Error::InvalidArgument(format!(
            "invalid journal url: {:?}",
            parsed_url
        ))),
    }
}
