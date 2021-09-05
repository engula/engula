use std::io::IoSlice;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::error::Result;
use crate::file_system::*;

pub struct LocalFileSystem {
    dirname: PathBuf,
}

impl LocalFileSystem {
    pub fn new<P: AsRef<Path>>(dirname: P) -> Result<LocalFileSystem> {
        std::fs::create_dir_all(dirname.as_ref())?;
        Ok(LocalFileSystem {
            dirname: dirname.as_ref().to_owned(),
        })
    }
}

#[async_trait]
impl FileSystem for LocalFileSystem {
    async fn new_sequential_reader(&self, fname: &str) -> Result<Box<dyn SequentialReader>> {
        let path = self.dirname.join(fname);
        let file = tokio::fs::File::open(path).await?;
        Ok(Box::new(SequentialFile::new(file)))
    }

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
    file: Pin<Box<tokio::fs::File>>,
}

impl SequentialFile {
    fn new(file: tokio::fs::File) -> SequentialFile {
        SequentialFile {
            file: Box::pin(file),
        }
    }
}

type IoResult<T> = std::result::Result<T, std::io::Error>;

impl AsyncRead for SequentialFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        self.file.as_mut().poll_read(cx, buf)
    }
}

impl AsyncWrite for SequentialFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        self.file.as_mut().poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.file.as_mut().poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.file.as_mut().poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<IoResult<usize>> {
        self.file.as_mut().poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.file.is_write_vectored()
    }
}

impl SequentialReader for SequentialFile {}

#[async_trait]
impl SequentialWriter for SequentialFile {
    async fn sync_data(&self) -> Result<()> {
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
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        // NOTE: this is a blocking read.
        let size = self.file.read_at(buf, offset)?;
        Ok(size)
    }
}
