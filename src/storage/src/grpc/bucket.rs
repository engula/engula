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
    io::{Error as IoError, ErrorKind},
    pin::Pin,
    task::{Context, Poll},
};

use futures::{stream::StreamExt, FutureExt};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::mpsc,
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::PollSender;
use tonic::Streaming;

use super::{
    client::Client,
    proto::{
        DeleteObjectRequest, ReadObjectRequest, ReadObjectResponse, UploadObjectRequest,
        UploadObjectResponse,
    },
};
use crate::{async_trait, Result};

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
        self.client.delete_object(input).await?;
        Ok(())
    }

    async fn new_sequential_reader(&self, name: &str) -> Result<Self::SequentialReader> {
        let input = ReadObjectRequest {
            bucket: self.bucket_name.to_owned(),
            object: name.to_owned(),
            offset: 0,
            length: i64::MAX,
        };
        let stream = self.client.read_object(input).await?;
        Ok(SequentialReader::new(stream))
    }

    async fn new_sequential_writer(&self, name: &str) -> Result<Self::SequentialWriter> {
        Ok(SequentialWriter::new(
            self.client.clone(),
            self.bucket_name.to_owned(),
            name.to_owned(),
        ))
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
    tx: PollSender<UploadObjectRequest>,
    upload: JoinHandle<Result<UploadObjectResponse>>,
    bucket_name: String,
    object_name: String,
}

impl SequentialWriter {
    fn new(client: Client, bucket_name: String, object_name: String) -> Self {
        let (tx, rx) = mpsc::channel(16);
        let tx = PollSender::new(tx);
        let rx = ReceiverStream::new(rx);
        let upload = tokio::spawn(async move { client.upload_object(rx).await });
        Self {
            tx,
            upload,
            bucket_name,
            object_name,
        }
    }
}

impl AsyncWrite for SequentialWriter {
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

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.tx.poll_send_done(cx) {
            Poll::Ready(ready) => match ready {
                Ok(()) => {
                    self.tx.close_this_sender();
                    self.upload.poll_unpin(cx).map(|ready| {
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
