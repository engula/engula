use std::{
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use super::{Fs, RandomAccessReader, SequentialWriter};
use crate::error::{Error, Result};

pub struct LocalFs {
    dirname: PathBuf,
}

impl LocalFs {
    pub fn new<P: AsRef<Path>>(dirname: P) -> Result<LocalFs> {
        std::fs::create_dir_all(&dirname)?;
        Ok(LocalFs {
            dirname: dirname.as_ref().to_owned(),
        })
    }
}

#[async_trait]
impl Fs for LocalFs {
    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn SequentialWriter>> {
        let path = self.dirname.join(fname);
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .await?;
        Ok(Box::new(SequentialFile::new(file)))
    }

    async fn new_random_access_reader(&self, fname: &str) -> Result<Box<dyn RandomAccessReader>> {
        let path = self.dirname.join(fname);
        let file = std::fs::File::open(path)?;
        Ok(Box::new(RandomAccessFile::new(file)))
    }

    async fn remove_file(&self, fname: &str) -> Result<()> {
        let path = self.dirname.join(fname);
        tokio::fs::remove_file(path).await?;
        Ok(())
    }
}

struct SequentialFile {
    file: tokio::fs::File,
    error: Option<Error>,
}

impl SequentialFile {
    fn new(file: tokio::fs::File) -> SequentialFile {
        SequentialFile { file, error: None }
    }
}

#[async_trait]
impl SequentialWriter for SequentialFile {
    async fn write(&mut self, data: Vec<u8>) {
        if self.error.is_some() {
            return;
        }
        if let Err(err) = self.file.write_all(&data).await {
            self.error = Some(err.into());
        }
    }

    async fn finish(&mut self) -> Result<()> {
        if let Some(err) = &self.error {
            return Err(err.clone());
        }
        self.file.sync_data().await?;
        Ok(())
    }
}

struct RandomAccessFile {
    file: std::fs::File,
}

impl RandomAccessFile {
    fn new(file: std::fs::File) -> RandomAccessFile {
        RandomAccessFile { file }
    }
}

#[async_trait]
impl RandomAccessReader for RandomAccessFile {
    async fn read_at(&self, offset: u64, size: u64) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.resize(size as usize, 0);
        // TODO: this is a blocking read.
        self.file.read_at(&mut buf, offset)?;
        Ok(buf)
    }
}
