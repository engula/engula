use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::Request;

use super::{
    file_system_client, AccessMode, FileSystem, FinishRequest, OpenRequest, RandomAccessReader,
    ReadRequest, RemoveRequest, SequentialWriter, WriteRequest,
};
use crate::error::Result;

type FileSystemClient = file_system_client::FileSystemClient<Channel>;

pub struct RemoteFileSystem {
    client: Mutex<FileSystemClient>,
}

impl RemoteFileSystem {
    async fn open_file(&self, fname: &str, mode: AccessMode) -> Result<RemoteFile> {
        let input = OpenRequest {
            file_name: fname.to_owned(),
            access_mode: mode as i32,
        };
        let request = Request::new(input);
        let mut client = self.client.lock().await;
        let response = client.open(request).await?;
        let output = response.into_inner();
        Ok(RemoteFile::new(output.fd, client.clone()))
    }
}

#[async_trait]
impl FileSystem for RemoteFileSystem {
    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn SequentialWriter>> {
        let file = self.open_file(fname, AccessMode::Write).await?;
        Ok(Box::new(file))
    }

    async fn new_random_access_reader(&self, fname: &str) -> Result<Box<dyn RandomAccessReader>> {
        let file = self.open_file(fname, AccessMode::Read).await?;
        Ok(Box::new(file))
    }

    async fn remove_file(&self, fname: &str) -> Result<()> {
        let input = RemoveRequest {
            file_name: fname.to_owned(),
        };
        let request = Request::new(input);
        let mut client = self.client.lock().await;
        client.remove(request).await?;
        Ok(())
    }
}

struct RemoteFile {
    fd: u64,
    client: FileSystemClient,
}

impl RemoteFile {
    fn new(fd: u64, client: FileSystemClient) -> RemoteFile {
        RemoteFile { fd, client }
    }
}

#[async_trait]
impl SequentialWriter for RemoteFile {
    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        let input = WriteRequest {
            fd: self.fd,
            data: buf.to_owned(),
        };
        let request = Request::new(input);
        let mut client = self.client.clone();
        client.write(request).await?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        let input = FinishRequest { fd: self.fd };
        let request = Request::new(input);
        let mut client = self.client.clone();
        client.finish(request).await?;
        Ok(())
    }
}

#[async_trait]
impl RandomAccessReader for RemoteFile {
    async fn read_at(&self, offset: u64, size: u64) -> Result<Vec<u8>> {
        let input = ReadRequest {
            fd: self.fd,
            offset,
            size,
        };
        let request = Request::new(input);
        let mut client = self.client.clone();
        let response = client.read(request).await?;
        let output = response.into_inner();
        Ok(output.data)
    }
}
