use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::StreamExt;
use metrics::histogram;
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    sync::{mpsc, Mutex},
    task,
    time::Instant,
};
use tokio_stream::wrappers::ReceiverStream;

use super::{write::WriteBatch, Journal, JournalOptions};
use crate::error::Result;

struct JournalFile {
    dirname: PathBuf,
    options: JournalOptions,
    file_number: usize,
    file: File,
    size: usize,
}

impl JournalFile {
    fn new<P: AsRef<Path>>(dirname: P, options: JournalOptions) -> Result<JournalFile> {
        let dirname = dirname.as_ref().to_owned();
        std::fs::create_dir_all(&dirname)?;
        let file = JournalFile::open_file(&dirname, 0)?;
        Ok(JournalFile {
            dirname,
            options,
            file_number: 0,
            file,
            size: 0,
        })
    }

    fn file_name(number: usize) -> String {
        format!("{}.log", number)
    }

    fn open_file<P: AsRef<Path>>(dirname: P, number: usize) -> Result<File> {
        let filename = JournalFile::file_name(number);
        let path = dirname.as_ref().join(filename);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(File::from_std(file))
    }

    fn next_file(&mut self) -> Result<()> {
        let previous_name = JournalFile::file_name(self.file_number);
        let previous_path = self.dirname.join(previous_name);
        self.file_number += 1;
        self.file = JournalFile::open_file(&self.dirname, self.file_number)?;
        self.size = 0;
        std::fs::remove_file(previous_path)?;
        Ok(())
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
            self.next_file()?;
        }
        Ok(())
    }
}

pub struct LocalJournal {
    file: Mutex<JournalFile>,
}

impl LocalJournal {
    pub fn new<P: AsRef<Path>>(dirname: P, options: JournalOptions) -> Result<LocalJournal> {
        let file = JournalFile::new(dirname, options)?;
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
