mod fs_service;
mod local_fs;
mod remote_fs;
mod s3_fs;

pub use fs_server::FsServer;
pub use fs_service::FsService;
pub use local_fs::LocalFs;
pub use proto::*;
pub use remote_fs::RemoteFs;
pub use s3_fs::{S3Fs, S3Options};

mod proto {
    tonic::include_proto!("engula.fs");
}

use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait Fs: Sync + Send {
    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn SequentialWriter>>;

    async fn new_random_access_reader(&self, fname: &str) -> Result<Box<dyn RandomAccessReader>>;

    async fn count_file(&self, _: &str) -> Result<usize> {
        // Only for S3
        Ok(0)
    }

    async fn remove_file(&self, fname: &str) -> Result<()>;
}

#[async_trait]
pub trait SequentialWriter: Sync + Send + Unpin {
    async fn write(&mut self, data: Vec<u8>);

    async fn finish(&mut self) -> Result<()>;

    fn suggest_buffer_size(&self) -> usize {
        1024 * 1024
    }
}

#[async_trait]
pub trait RandomAccessReader: Sync + Send + Unpin {
    async fn read_at(&self, offset: u64, size: u64) -> Result<Vec<u8>>;
}
