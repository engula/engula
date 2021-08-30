use std::io::IoSlice;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::file_system::*;
use crate::error::Result;

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

    async fn open_sequential_file(&self, fname: &str) -> Result<Box<LocalSequentialFile>> {
        let path = self.dirname.join(fname);
        let file = tokio::fs::File::open(path).await?;
        Ok(Box::new(LocalSequentialFile::new(file)))
    }

    async fn open_random_access_file(&self, fname: &str) -> Result<Box<LocalRandomAccessFile>> {
        let path = self.dirname.join(fname);
        let file = std::fs::File::open(path)?;
        Ok(Box::new(LocalRandomAccessFile::new(file)))
    }
}

#[async_trait]
impl FileSystem for LocalFileSystem {
    async fn new_sequential_reader(&self, fname: &str) -> Result<Box<dyn SequentialReader>> {
        let file = self.open_sequential_file(fname).await?;
        Ok(file)
    }

    async fn new_random_access_reader(&self, fname: &str) -> Result<Box<dyn RandomAccessReader>> {
        let file = self.open_random_access_file(fname).await?;
        Ok(file)
    }

    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn SequentialWriter>> {
        let file = self.open_sequential_file(fname).await?;
        Ok(file)
    }
}

struct LocalSequentialFile {
    file: Pin<Box<tokio::fs::File>>,
}

impl LocalSequentialFile {
    fn new(file: tokio::fs::File) -> LocalSequentialFile {
        LocalSequentialFile {
            file: Box::pin(file),
        }
    }
}

type IoResult<T> = std::result::Result<T, std::io::Error>;

impl AsyncRead for LocalSequentialFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        self.file.as_mut().poll_read(cx, buf)
    }
}

impl AsyncWrite for LocalSequentialFile {
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

impl SequentialReader for LocalSequentialFile {}

#[async_trait]
impl SequentialWriter for LocalSequentialFile {
    async fn sync_data(&self) -> Result<()> {
        self.file.sync_data().await?;
        Ok(())
    }
}

pub struct LocalRandomAccessFile {
    file: std::fs::File,
}

impl LocalRandomAccessFile {
    fn new(file: std::fs::File) -> LocalRandomAccessFile {
        LocalRandomAccessFile { file }
    }
}

#[async_trait]
impl RandomAccessReader for LocalRandomAccessFile {
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let size = self.file.read_at(buf, offset)?;
        Ok(size)
    }
}
