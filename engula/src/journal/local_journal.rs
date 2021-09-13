use std::path::Path;

use async_trait::async_trait;
use futures::StreamExt;
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    sync::{mpsc, Mutex},
    task,
};
use tokio_stream::wrappers::ReceiverStream;

use super::{write::WriteBatch, Journal, JournalOptions};
use crate::error::Result;

struct JournalFile {
    file: File,
    sync: bool,
}

impl JournalFile {
    fn new(file: std::fs::File, sync: bool) -> JournalFile {
        JournalFile {
            file: File::from_std(file),
            sync,
        }
    }

    async fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_all(data).await?;
        if self.sync {
            self.file.sync_data().await?;
        }
        Ok(())
    }
}

pub struct LocalJournal {
    options: JournalOptions,
    file: Mutex<JournalFile>,
}

impl LocalJournal {
    pub fn new<P: AsRef<Path>>(dirname: P, options: JournalOptions) -> Result<LocalJournal> {
        let filename = dirname.as_ref().join("engula.log");
        std::fs::create_dir_all(dirname)?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(filename)?;
        let file = JournalFile::new(file, options.sync);
        Ok(LocalJournal {
            options,
            file: Mutex::new(file),
        })
    }

    pub async fn append(&self, data: &[u8]) -> Result<()> {
        let mut file = self.file.lock().await;
        file.append(data).await
    }
}

#[async_trait]
impl Journal for LocalJournal {
    async fn append_stream(&self, rx: mpsc::Receiver<WriteBatch>) -> Result<()> {
        let mut file = self.file.lock().await;
        let mut buffer = Vec::with_capacity(1024 * 1024);
        let mut stream = ReceiverStream::new(rx).ready_chunks(self.options.chunk_size);
        while let Some(mut batches) = stream.next().await {
            buffer.clear();
            for batch in &mut batches {
                buffer.append(&mut batch.buffer);
            }
            file.append(&buffer).await?;
            for batch in batches {
                batch.tx.send(batch.writes).await?;
            }
            task::yield_now().await;
        }
        Ok(())
    }
}
