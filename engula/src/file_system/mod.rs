mod local_file_system;
mod remote_file_system;
mod s3;
mod service;

pub use local_file_system::LocalFileSystem;
pub use remote_file_system::RemoteFileSystem;
pub use s3::{S3Bucket, S3Config};
pub use service::Service as FileSystemService;

tonic::include_proto!("engula.file_system");

use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait FileSystem: Sync + Send {
    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn SequentialWriter>>;

    async fn new_random_access_reader(&self, fname: &str) -> Result<Box<dyn RandomAccessReader>>;

    async fn remove_file(&self, fname: &str) -> Result<()>;
}

#[async_trait]
pub trait SequentialWriter: Sync + Send + Unpin {
    async fn write(&mut self, data: &[u8]) -> Result<()>;

    async fn sync(&mut self) -> Result<()>;
}

#[async_trait]
pub trait RandomAccessReader: Sync + Send + Unpin {
    async fn read_at(&self, offset: u64, size: u64) -> Result<Vec<u8>>;
}
