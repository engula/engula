use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::Result;

#[async_trait]
pub trait FileSystem: Sync + Send {
    async fn new_sequential_reader(&self, fname: &str) -> Result<Box<dyn SequentialReader>>;

    async fn new_random_access_reader(&self, fname: &str) -> Result<Box<dyn RandomAccessReader>>;

    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn SequentialWriter>>;

    async fn remove_file(&self, fname: &str) -> Result<()>;
}

pub trait SequentialReader: Sync + Send + Unpin + AsyncRead {}

#[async_trait]
pub trait RandomAccessReader: Sync + Send + Unpin {
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
}

#[async_trait]
pub trait SequentialWriter: Sync + Send + Unpin + AsyncWrite {
    async fn sync_data(&self) -> Result<()>;
}
