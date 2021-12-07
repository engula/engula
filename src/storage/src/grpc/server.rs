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

use futures::{Stream, StreamExt, TryStreamExt};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use tonic::{Request, Response, Status, Streaming};

use super::proto::*;
use crate::{Bucket, Storage};

pub struct Server<S: Storage> {
    storage: S,
}

impl<S: Storage> Server<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    pub fn into_service(self) -> storage_server::StorageServer<Server<S>> {
        storage_server::StorageServer::new(self)
    }
}

#[tonic::async_trait]
impl<S: Storage> storage_server::Storage for Server<S> {
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
        let mut stream = request.into_inner();
        let mut cw = None;
        while let Some(req) = stream.try_next().await? {
            if cw.is_none() {
                let b = self.storage.bucket(&req.bucket).await?;
                let w = b.new_sequential_writer(&req.object).await?;
                cw = Some(w);
            }
            if let Some(w) = &mut cw {
                w.write_all(&req.content).await?;
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
        let byte_stream = ReaderStream::new(r);
        let resp_stream = byte_stream.map(move |res| {
            res.map(|b| ReadObjectResponse {
                content: b.to_vec(),
            })
            .map_err(|e| e.into())
        });
        Ok(Response::new(resp_stream))
    }
}
