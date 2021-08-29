use std::io::IoSlice;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::file_system::*;
use crate::Result;

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
    async fn new_sequential_reader(&self, fname: &str) -> Result<Box<dyn SequentialFileReader>> {
        let path = self.dirname.join(fname);
        let file = File::open(path).await?;
        Ok(Box::new(LocalSequentialFile::new(file)))
    }

    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn SequentialFileWriter>> {
        let path = self.dirname.join(fname);
        let file = File::open(path).await?;
        Ok(Box::new(LocalSequentialFile::new(file)))
    }
}

struct LocalSequentialFile {
    file: Pin<Box<File>>,
}

impl LocalSequentialFile {
    fn new(file: File) -> LocalSequentialFile {
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

impl SequentialFileReader for LocalSequentialFile {}

impl SequentialFileWriter for LocalSequentialFile {}
