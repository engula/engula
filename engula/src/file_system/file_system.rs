use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::Result;

#[async_trait]
pub trait FileSystem: Sync + Send {
    async fn new_sequential_reader(&self, fname: &str) -> Result<Box<dyn SequentialFileReader>>;

    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn SequentialFileWriter>>;
}

pub trait SequentialFileReader: Sync + Send + Unpin + AsyncRead {}

pub trait SequentialFileWriter: Sync + Send + Unpin + AsyncWrite {}
