// Copyright 2021 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    cell::RefCell,
    io::{self, Error as IoError, ErrorKind},
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes};
use engula_futures::{io::RandomRead, stream::batch::VecResultStream};
use futures::{future, stream::StreamExt, Future, FutureExt};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::PollSender;

use super::{client::Client, proto::*};
use crate::{async_trait, storage::WriteOption, Error, Result};

#[derive(Clone)]
pub struct Storage {
    client: Client,
}

impl Storage {
    #[allow(dead_code)]
    pub async fn connect(addr: &str) -> Result<Storage> {
        let endpoint = format!("http://{}", addr);
        let client = Client::connect(&endpoint).await?;
        Ok(Storage { client })
    }
}

#[async_trait]
impl crate::Storage for Storage {
    type BucketLister = VecResultStream<String, Error>;
    type ObjectLister = VecResultStream<String, Error>;
    type RandomReader = RandomReader;
    type SequentialWriter = SequentialWriter;

    async fn create_bucket(&self, bucket_name: &str) -> Result<()> {
        self.client
            .create_bucket(CreateBucketRequest {
                bucket: bucket_name.to_owned(),
            })
            .await?;
        Ok(())
    }

    async fn delete_bucket(&self, bucket_name: &str) -> Result<()> {
        self.client
            .delete_bucket(DeleteBucketRequest {
                bucket: bucket_name.to_owned(),
            })
            .await?;
        Ok(())
    }

    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()> {
        self.client
            .delete_object(DeleteObjectRequest {
                bucket: bucket_name.to_owned(),
                object: object_name.to_owned(),
            })
            .await?;
        Ok(())
    }

    async fn list_buckets(&self) -> Result<Self::BucketLister> {
        let stream = self.client.list_buckets(ListBucketsRequest {}).await?;
        let resps = stream
            .map(|res| res.map(|resp| resp.bucket))
            .collect::<Vec<_>>()
            .await;
        let mut result = Vec::new();
        for resp in resps {
            result.push(resp?)
        }
        Ok(VecResultStream::new(result))
    }

    async fn list_objects(&self, bucket_name: &str) -> Result<Self::ObjectLister> {
        let stream = self
            .client
            .list_objects(ListObjectsRequest {
                bucket: bucket_name.to_owned(),
            })
            .await?;
        let resps = stream
            .map(|res| res.map(|resp| resp.object))
            .collect::<Vec<_>>()
            .await;
        let mut result = Vec::new();
        for resp in resps {
            result.push(resp?)
        }
        Ok(VecResultStream::new(result))
    }

    async fn new_random_reader(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::RandomReader> {
        Ok(RandomReader::new(
            self.client.clone(),
            bucket_name.to_owned(),
            object_name.to_owned(),
        ))
    }

    async fn new_sequential_writer(
        &self,
        bucket_name: &str,
        object_name: &str,
        option: WriteOption,
    ) -> Result<Self::SequentialWriter> {
        Ok(SequentialWriter::new(
            self.client.clone(),
            bucket_name.to_owned(),
            object_name.to_owned(),
            option,
        ))
    }
}

type IoResult<T> = std::result::Result<T, IoError>;

pub struct RandomReader {
    client: Client,
    bucket: String,
    key: String,

    inner: RefCell<ReaderInner>,
}

type PinnedFuture<T, E> = Pin<Box<dyn Future<Output = std::result::Result<T, E>> + 'static + Send>>;

pub struct ReaderInner {
    get_obj_fut: Option<PinnedFuture<Bytes, io::Error>>,
}

impl RandomReader {
    fn new(client: Client, bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            key: key.into(),
            inner: RefCell::new(ReaderInner { get_obj_fut: None }),
        }
    }

    async fn get_object(
        client: Client,
        bucket: impl Into<String>,
        key: impl Into<String>,
        pos: usize,
        len: usize,
    ) -> io::Result<Bytes> {
        let input = ReadObjectRequest {
            bucket: bucket.into(),
            object: key.into(),
            pos: pos as i32,
        };
        let stream = client
            .read_object(input)
            .await
            .map_err(|e| IoError::new(ErrorKind::Other, format!("{:?}", e)))?;
        let mut remain_bytes = len as i32;
        let mut meet_err = false;
        let byte_stream = stream
            .map(|res| {
                res.map(|resp| Bytes::from(resp.content))
                    .map_err(|s| IoError::new(ErrorKind::Other, format!("{:?}", s)))
            })
            .take_while(|r| {
                if meet_err || remain_bytes <= 0 {
                    return future::ready(false);
                }
                match r {
                    Ok(b) => remain_bytes -= b.len() as i32,
                    Err(_) => meet_err = true,
                }
                future::ready(true)
            });
        let bs = byte_stream.collect::<Vec<_>>().await;
        let mut result = Vec::new();
        for b in bs {
            result.push(b?);
        }
        Ok(Bytes::from(result.concat()))
    }
}

impl RandomRead for RandomReader {
    fn poll_read(
        self: Pin<&Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
        pos: usize,
    ) -> Poll<io::Result<usize>> {
        let this = self.get_ref();
        let mut inner = this.inner.borrow_mut();
        let mut fut = inner.get_obj_fut.take().unwrap_or_else(|| {
            Box::pin(Self::get_object(
                this.client.clone(),
                this.bucket.to_owned(),
                this.key.to_owned(),
                pos,
                buf.len(),
            ))
        });

        match fut.as_mut().poll(cx) {
            Poll::Pending => {
                inner.get_obj_fut = Some(fut);
                Poll::Pending
            }
            Poll::Ready(Ok(mut bytes)) => {
                let size = bytes.len();
                bytes.copy_to_slice(&mut buf[0..size]);
                inner.get_obj_fut = None;
                Poll::Ready(Ok(size))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }
}

pub struct SequentialWriter {
    tx: PollSender<UploadObjectRequest>,
    upload: JoinHandle<Result<UploadObjectResponse>>,
    bucket_name: String,
    object_name: String,
    write_option: WriteOption,
}

impl SequentialWriter {
    fn new(
        client: Client,
        bucket_name: String,
        object_name: String,
        write_option: WriteOption,
    ) -> Self {
        let (tx, rx) = mpsc::channel(16);
        let tx = PollSender::new(tx);
        let rx = ReceiverStream::new(rx);
        let upload = tokio::spawn(async move { client.upload_object(rx).await });
        Self {
            tx,
            upload,
            bucket_name,
            object_name,
            write_option,
        }
    }
}

impl engula_futures::io::SequentialWrite for SequentialWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        self.tx.poll_send_done(cx).map(|ready| {
            ready
                .and_then(|_| {
                    let req = UploadObjectRequest {
                        bucket: self.bucket_name.clone(),
                        object: self.object_name.clone(),
                        content: buf.to_owned(),
                        replica_request: self.write_option.replica_write,
                        replica_chain: self.write_option.replica_chain.clone(),
                    };
                    self.tx.start_send(req)
                })
                .map(|_| buf.len())
                .map_err(|err| IoError::new(ErrorKind::Other, err.to_string()))
        })
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<IoResult<()>> {
        // Not sure what guarantee we should provide here yet.
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self.as_mut();
        match this.tx.poll_send_done(cx) {
            Poll::Ready(ready) => match ready {
                Ok(()) => {
                    this.tx.close_this_sender();
                    this.upload.poll_unpin(cx).map(|ready| {
                        ready
                            .map(|_| ())
                            .map_err(|err| IoError::new(ErrorKind::Other, err.to_string()))
                    })
                }
                Err(err) => Poll::Ready(Err(IoError::new(ErrorKind::Other, err.to_string()))),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}
