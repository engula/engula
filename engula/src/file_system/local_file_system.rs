use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite};

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
    async fn new_sequential_reader(&self, fname: &str) -> Result<Box<dyn AsyncRead>> {
        let path = self.dirname.join(fname);
        let file = File::open(path).await?;
        Ok(Box::new(file))
    }

    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn AsyncWrite>> {
        let path = self.dirname.join(fname);
        let file = File::open(path).await?;
        Ok(Box::new(file))
    }
}
