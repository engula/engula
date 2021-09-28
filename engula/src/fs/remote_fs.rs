use async_trait::async_trait;
use futures::StreamExt;
use tokio::{
    sync::{mpsc, oneshot},
    task,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, Request};
use tracing::error;

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
}

#[async_trait]
impl Fs for RemoteFs {
    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn SequentialWriter>> {
        let mut client = self.client.clone();
        let input = OpenRequest {
            file_name: fname.to_owned(),
            access_mode: AccessMode::Write as i32,
        };
        let request = Request::new(input);
        match client.open(request).await {
            Ok(response) => {
                let output = response.into_inner();
                let writer = RemoteWriter::new(output.fd, client.clone());
                Ok(Box::new(writer))
            }
            Err(err) => {
                error!("open {}: {}", fname, err);
                Err(err.into())
            }
        }
    }

    async fn new_random_access_reader(&self, fname: &str) -> Result<Box<dyn RandomAccessReader>> {
        let mut client = self.client.clone();
        let input = OpenRequest {
            file_name: fname.to_owned(),
            access_mode: AccessMode::Read as i32,
        };
        let request = Request::new(input);
        match client.open(request).await {
            Ok(response) => {
                let output = response.into_inner();
                let reader = RemoteReader::new(output.fd, client.clone());
                Ok(Box::new(reader))
            }
            Err(err) => {
                error!("open {}: {}", fname, err);
                Err(err.into())
            }
        }
    }

    async fn remove_file(&self, fname: &str) -> Result<()> {
        let mut client = self.client.clone();
        let input = RemoveRequest {
            file_name: fname.to_owned(),
        };
        let request = Request::new(input);
        match client.remove(request).await {
            Ok(_) => Ok(()),
            Err(err) => {
                error!("remove {}: {}", fname, err);
                Err(err.into())
            }
        }
    }
}

struct RemoteWriter {
    fd: u64,
    client: FsClient,
    write_tx: Option<mpsc::Sender<WriteRequest>>,
    write_handle: Option<task::JoinHandle<Result<()>>>,
}

impl RemoteWriter {
    fn new(fd: u64, client: FsClient) -> RemoteWriter {
        let mut client_clone = client.clone();
        let (tx, rx) = mpsc::channel(1024);
        let handle = task::spawn(async move {
            let stream = ReceiverStream::new(rx);
            let request = Request::new(stream);
            client_clone.write(request).await?;
            Ok(())
        });

        RemoteWriter {
            fd,
            client,
            write_tx: Some(tx),
            write_handle: Some(handle),
        }
    }
}

#[async_trait]
impl SequentialWriter for RemoteWriter {
    async fn write(&mut self, data: Vec<u8>) {
        let write = WriteRequest { fd: self.fd, data };
        if let Some(tx) = &self.write_tx {
            tx.send(write).await.unwrap();
        }
    }

    async fn finish(&mut self) -> Result<()> {
        self.write_tx.take();
        self.write_handle.take().unwrap().await??;
        let input = FinishRequest { fd: self.fd };
        let request = Request::new(input);
        self.client.finish(request).await?;
        Ok(())
    }
}

struct Read {
    tx: oneshot::Sender<Vec<u8>>,
    offset: u64,
    size: u64,
}

struct RemoteReader {
    read_tx: mpsc::Sender<Read>,
}

impl RemoteReader {
    fn new(fd: u64, mut client: FsClient) -> RemoteReader {
        let (tx, rx): (mpsc::Sender<Read>, mpsc::Receiver<Read>) = mpsc::channel(1024);
        let _: task::JoinHandle<Result<()>> = task::spawn(async move {
            let mut stream = ReceiverStream::new(rx).ready_chunks(1024);
            while let Some(reads) = stream.next().await {
                let mut offsets = Vec::with_capacity(reads.len());
                let mut sizes = Vec::with_capacity(reads.len());
                for read in &reads {
                    offsets.push(read.offset);
                    sizes.push(read.size);
                }
                let input = ReadRequest { fd, offsets, sizes };
                let request = Request::new(input);
                let response = client.read(request).await?;
                let output = response.into_inner();
                for (read, data) in reads.into_iter().zip(output.data) {
                    read.tx.send(data).unwrap();
                }
            }
            Ok(())
        });
        RemoteReader { read_tx: tx }
    }
}

#[async_trait]
impl RandomAccessReader for RemoteReader {
    async fn read_at(&self, offset: u64, size: u64) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let read = Read { tx, offset, size };
        self.read_tx.send(read).await?;
        let data = rx.await?;
        Ok(data)
    }
}
