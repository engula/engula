use std::{io::SeekFrom, path::Path};

use async_trait::async_trait;
use futures::StreamExt;
use metrics::histogram;
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{mpsc, Mutex},
    task,
    time::Instant,
};
use tokio_stream::wrappers::ReceiverStream;

use super::{write::WriteBatch, Journal, JournalOptions};
use crate::error::Result;

struct JournalFile {
    options: JournalOptions,
    file: File,
    size: usize,
}

impl JournalFile {
    fn new(options: JournalOptions, file: std::fs::File) -> JournalFile {
        JournalFile {
            options,
            file: File::from_std(file),
            size: 0,
        }
    }

    async fn append(&mut self, data: &[u8]) -> Result<()> {
        let start = Instant::now();
        self.file.write_all(data).await?;
        if self.options.sync {
            self.file.sync_data().await?;
        }
        histogram!("engula.journal.local.append.seconds", start.elapsed());
        self.size += data.len();
        if self.size >= self.options.size {
            self.file.seek(SeekFrom::Start(0)).await?;
        }
        Ok(())
    }
}

pub struct LocalJournal {
    file: Mutex<JournalFile>,
}

impl LocalJournal {
    pub fn new<P: AsRef<Path>>(dirname: P, options: JournalOptions) -> Result<LocalJournal> {
        std::fs::create_dir_all(&dirname)?;
        let filename = dirname.as_ref().join("engula.log");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(filename)?;
        let file = JournalFile::new(options, file);
        Ok(LocalJournal {
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
        let mut stream = ReceiverStream::new(rx).ready_chunks(1024);
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
