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
    io::{Error as IoError, ErrorKind, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    ready,
    stream::{self, StreamExt},
    Future, FutureExt,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::mpsc::{self, error::TrySendError, Receiver, Sender},
};
use tonic::{IntoStreamingRequest, Request, Streaming};

use super::{
    client::Client,
    proto::{
        DeleteObjectRequest, ReadObjectRequest, ReadObjectResponse, UploadObjectRequest,
        UploadObjectResponse,
    },
};
use crate::{async_trait, Error, Result};

#[derive(Clone)]
pub struct Bucket {
    client: Client,
    bucket_name: String,
}

impl Bucket {
    pub fn new(client: Client, bucket_name: impl Into<String>) -> Self {
        Self {
            client,
            bucket_name: bucket_name.into(),
        }
    }

    async fn init_upload(
        client: Client,
        req: impl IntoStreamingRequest<Message = UploadObjectRequest>,
    ) -> std::result::Result<UploadObjectResponse, Error> {
        client.upload_object(req).await
    }
}

#[async_trait]
impl crate::Bucket for Bucket {
    type SequentialReader = SequentialReader;
    type SequentialWriter = SequentialWriter;

    async fn delete_object(&self, name: &str) -> Result<()> {
        let input = DeleteObjectRequest {
            bucket: self.bucket_name.to_owned(),
            object: name.to_owned(),
        };
        let _ = self.client.delete_object(input).await?;
        Ok(())
    }

    async fn new_sequential_reader(&self, name: &str) -> Result<Self::SequentialReader> {
        let input = ReadObjectRequest {
            bucket: self.bucket_name.to_owned(),
            object: name.to_owned(),
            offset: 0_i64,
            length: i64::MAX,
        };
        let s = self.client.read_object(input).await?;
        Ok(SequentialReader::new(s))
    }

    async fn new_sequential_writer(&self, name: &str) -> Result<Self::SequentialWriter> {
        let (tx, rx) = mpsc::channel(1);
        let bucket_name = self.bucket_name.to_owned();
        let object_name = name.to_owned();

        struct WriteCtx {
            rx: Receiver<Vec<u8>>,
            bucket_name: String,
            object_name: String,
        }

        let init = WriteCtx {
            rx,
            bucket_name,
            object_name,
        };
        let is = stream::unfold(init, |mut s| async move {
            match s.rx.recv().await {
                Some(up) => Some((
                    UploadObjectRequest {
                        bucket: s.bucket_name.to_owned(),
                        object: s.object_name.to_owned(),
                        content: up,
                    },
                    s,
                )),
                None => None,
            }
        });
        let req = Request::new(is);
        let upload_fut = Self::init_upload(self.client.clone(), req);
        Ok(SequentialWriter::new(tx, Box::pin(upload_fut)))
    }
}

type IoResult<T> = std::result::Result<T, IoError>;

pub struct SequentialReader {
    stream: Streaming<ReadObjectResponse>,
}

impl SequentialReader {
    fn new(stream: Streaming<ReadObjectResponse>) -> Self {
        Self { stream }
    }
}

impl AsyncRead for SequentialReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(m))) => {
                buf.put_slice(&m.content);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Err(IoError::new(ErrorKind::Other, format!("{:?}", e))))
            }
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct SequentialWriter {
    tx: Option<Sender<Vec<u8>>>,
    fut: Pin<Box<dyn Future<Output = std::result::Result<UploadObjectResponse, Error>> + Send>>,
    buf: Vec<u8>,
}

impl SequentialWriter {
    fn new(
        tx: Sender<Vec<u8>>,
        fut: Pin<Box<dyn Future<Output = std::result::Result<UploadObjectResponse, Error>> + Send>>,
    ) -> Self {
        Self {
            tx: Some(tx),
            fut,
            buf: Vec::new(),
        }
    }
}

impl AsyncWrite for SequentialWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        Pin::new(&mut self.buf).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match ready!(Pin::new(&mut self.buf).poll_flush(cx)) {
            Ok(_) => match &self.tx {
                Some(tx) => match tx.try_send(self.buf.clone()) {
                    Ok(_) => {
                        Pin::new(&mut self.buf).clear();
                        Poll::Ready(Ok(()))
                    }
                    Err(TrySendError::Full(_)) => match Pin::new(&mut self).fut.poll_unpin(cx) {
                        Poll::Ready(_) => Poll::Pending,
                        Poll::Pending => Poll::Pending,
                    },
                    Err(TrySendError::Closed(_)) => Poll::Ready(Err(IoError::new(
                        ErrorKind::Other,
                        "unexpect channel closed",
                    ))),
                },
                None => Poll::Ready(Err(IoError::new(
                    ErrorKind::Other,
                    "flush on shutdown writer",
                ))),
            },
            Err(e) => Poll::Ready(Err(IoError::new(ErrorKind::Other, e.to_string()))),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match ready!(Pin::new(&mut self.buf).poll_shutdown(cx)) {
            Ok(_) => {
                if !self.buf.is_empty() {
                    if let Some(tx) = &self.tx {
                        if tx.try_send(self.buf.clone()).is_ok() {
                            Pin::new(&mut self.buf).clear();
                        }
                    }
                }
                let p = Pin::new(&mut self.fut).poll_unpin(cx);
                if self.buf.is_empty() {
                    self.tx = None;
                }
                match p {
                    Poll::Ready(_) => Poll::Ready(Ok(())),
                    Poll::Pending => Poll::Pending,
                }
            }
            Err(e) => Poll::Ready(Err(IoError::new(ErrorKind::Other, e.to_string()))),
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<IoResult<usize>> {
        Pin::new(&mut self.buf).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.buf.is_write_vectored()
    }
}
