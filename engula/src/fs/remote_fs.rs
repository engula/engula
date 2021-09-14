use async_trait::async_trait;
use tonic::{transport::Channel, Request};

use super::{proto::*, Fs, RandomAccessReader, SequentialWriter};
use crate::error::Result;

type FsClient = fs_client::FsClient<Channel>;

pub struct RemoteFs {
    client: FsClient,
}

impl RemoteFs {
    pub async fn new(url: &str) -> Result<RemoteFs> {
        let client = FsClient::connect(url.to_owned()).await?;
        Ok(RemoteFs { client })
    }

    async fn open_file(&self, fname: &str, mode: AccessMode) -> Result<RemoteFile> {
        let mut client = self.client.clone();
        let input = OpenRequest {
            file_name: fname.to_owned(),
            access_mode: mode as i32,
        };
        let request = Request::new(input);
        let response = client.open(request).await?;
        let output = response.into_inner();
        Ok(RemoteFile::new(output.fd, client.clone()))
    }
}

#[async_trait]
impl Fs for RemoteFs {
    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn SequentialWriter>> {
        let file = self.open_file(fname, AccessMode::Write).await?;
        Ok(Box::new(file))
    }

    async fn new_random_access_reader(&self, fname: &str) -> Result<Box<dyn RandomAccessReader>> {
        let file = self.open_file(fname, AccessMode::Read).await?;
        Ok(Box::new(file))
    }

    async fn remove_file(&self, fname: &str) -> Result<()> {
        let mut client = self.client.clone();
        let input = RemoveRequest {
            file_name: fname.to_owned(),
        };
        let request = Request::new(input);
        client.remove(request).await?;
        Ok(())
    }
}

struct RemoteFile {
    fd: u64,
    client: FsClient,
}

impl RemoteFile {
    fn new(fd: u64, client: FsClient) -> RemoteFile {
        RemoteFile { fd, client }
    }
}

#[async_trait]
impl SequentialWriter for RemoteFile {
    async fn write(&mut self, data: Vec<u8>) -> Result<()> {
        let input = WriteRequest { fd: self.fd, data };
        let request = Request::new(input);
        self.client.write(request).await?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        let input = FinishRequest { fd: self.fd };
        let request = Request::new(input);
        self.client.finish(request).await?;
        Ok(())
    }
}

#[async_trait]
impl RandomAccessReader for RemoteFile {
    async fn read_at(&self, offset: u64, size: u64) -> Result<Vec<u8>> {
        let mut client = self.client.clone();
        let input = ReadRequest {
            fd: self.fd,
            offset,
            size,
        };
        let request = Request::new(input);
        let response = client.read(request).await?;
        let output = response.into_inner();
        Ok(output.data)
    }
}
