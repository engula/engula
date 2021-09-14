mod fs_service;
mod local_fs;
mod remote_fs;
mod s3;

pub use fs_server::FsServer;
pub use fs_service::FsService;
pub use local_fs::LocalFs;
pub use proto::*;
pub use remote_fs::RemoteFs;
pub use s3::{S3Bucket, S3Config};

mod proto {
    tonic::include_proto!("engula.fs");
}

use async_trait::async_trait;
use url::Url;

use crate::error::{Error, Result};

#[async_trait]
pub trait Fs: Sync + Send {
    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn SequentialWriter>>;

    async fn new_random_access_reader(&self, fname: &str) -> Result<Box<dyn RandomAccessReader>>;

    async fn remove_file(&self, fname: &str) -> Result<()>;
}

#[async_trait]
pub trait SequentialWriter: Sync + Send + Unpin {
    async fn write(&mut self, data: Vec<u8>) -> Result<()>;

    async fn finish(&mut self) -> Result<()>;
}

#[async_trait]
pub trait RandomAccessReader: Sync + Send + Unpin {
    async fn read_at(&self, offset: u64, size: u64) -> Result<Vec<u8>>;
}

pub async fn open_fs(url: &str) -> Result<Box<dyn Fs>> {
    let parsed_url = Url::parse(url)?;
    match parsed_url.scheme() {
        "file" => {
            let fs = LocalFs::new(parsed_url.path())?;
            Ok(Box::new(fs))
        }
        "http" => {
            let fs = RemoteFs::new(url).await?;
            Ok(Box::new(fs))
        }
        _ => Err(Error::InvalidArgument(format!(
            "invalid fs url: {:?}",
            parsed_url
        ))),
    }
}
