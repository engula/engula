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

use std::marker::PhantomData;

use futures::{stream, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tonic::{Request, Response, Status, Streaming};

use super::proto::*;
use crate::{Bucket, Storage};

pub struct Server<B, S>
where
    B: Bucket,
    S: Storage,
{
    storage: S,
    _bucket: PhantomData<B>,
}

impl<B, S> Server<B, S>
where
    B: Bucket + Send + Sync + 'static,
    B::SequentialWriter: AsyncWrite + Send + Sync + 'static,
    B::SequentialReader: AsyncRead + Send + Sync + 'static,
    S: Storage<Bucket = B> + Send + Sync + 'static,
{
    pub fn new(storage: S) -> Self {
        Server {
            storage,
            _bucket: PhantomData,
        }
    }

    pub fn into_service(self) -> storage_server::StorageServer<Server<B, S>> {
        storage_server::StorageServer::new(self)
    }
}

#[tonic::async_trait]
impl<B, S> storage_server::Storage for Server<B, S>
where
    B: Bucket + Send + Sync + 'static,
    B::SequentialWriter: AsyncWrite + Send + Sync + 'static,
    B::SequentialReader: AsyncRead + Send + Sync + 'static,
    S: Storage<Bucket = B> + Send + Sync + 'static,
{
    type ReadObjectStream = impl Stream<Item = std::result::Result<ReadObjectResponse, Status>>;

    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketResponse>, Status> {
        let input = request.into_inner();
        self.storage.create_bucket(&input.bucket).await?;
        Ok(Response::new(CreateBucketResponse {}))
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        let input = request.into_inner();
        self.storage.delete_bucket(&input.bucket).await?;
        Ok(Response::new(DeleteBucketResponse {}))
    }

    async fn upload_object(
        &self,
        request: Request<Streaming<UploadObjectRequest>>,
    ) -> Result<Response<UploadObjectResponse>, Status> {
        let mut stream = request.into_inner().ready_chunks(1);
        let mut cw: Option<B::SequentialWriter> = None;
        while let Some(reqs) = stream.next().await {
            for req in reqs {
                let req = req?;
                if cw.is_none() {
                    let b = self.storage.bucket(&req.bucket).await?;
                    let w = b.new_sequential_writer(&req.object).await?;
                    cw = Some(w);
                }
                if let Some(w) = &mut cw {
                    w.write(&req.content).await?;
                }
            }
        }
        if let Some(w) = &mut cw {
            w.shutdown().await?;
        }
        Ok(Response::new(UploadObjectResponse {}))
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let input = request.into_inner();
        let b = self.storage.bucket(&input.bucket).await?;
        b.delete_object(&input.object).await?;
        Ok(Response::new(DeleteObjectResponse {}))
    }

    async fn read_object(
        &self,
        request: Request<ReadObjectRequest>,
    ) -> Result<Response<Self::ReadObjectStream>, Status> {
        let input = request.into_inner();
        let b = self.storage.bucket(&input.bucket).await?;
        let r = b.new_sequential_reader(&input.object).await?;
        let batch_size = core::cmp::min(input.length, 1024);
        let req_size = input.length;

        struct ReadCtx<B: Bucket> {
            r: B::SequentialReader,
            batch_size: i64,
            req_size: i64,
        }

        let init_state = ReadCtx::<B> {
            r,
            batch_size,
            req_size,
        };
        let stream = stream::unfold(init_state, |mut s| async move {
            let mut buf = vec![0; s.batch_size as usize];
            let result = s.r.read(&mut buf).await;
            match result {
                Ok(n) => {
                    if n == 0 || s.req_size == 0 {
                        None
                    } else {
                        let mut cut = buf.len();
                        if n < buf.len() {
                            cut = n;
                        }
                        let output = ReadObjectResponse {
                            content: buf[..cut].to_owned(),
                        };
                        Some((
                            Ok(output),
                            ReadCtx::<B> {
                                r: s.r,
                                batch_size: s.batch_size,
                                req_size: s.req_size - cut as i64,
                            },
                        ))
                    }
                }
                Err(e) => {
                    let status: Status = e.into();
                    Some((Err(status), s))
                }
            }
        });

        Ok(Response::new(stream))
    }
}
