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

use std::task::Poll;

use engula_futures::{io::ReadFromPosExt, stream::batch::ResultStreamExt};
use futures::{AsyncWriteExt, Stream, StreamExt, TryStreamExt};
use tokio_util::io::ReaderStream;
use tonic::{Request, Response, Status, Streaming};

use super::proto::ListBucketsRequest;
use crate::{remote::grpc::proto::*, storage::WriteOption, Storage};

pub struct Server<S: Storage> {
    storage: S,
}

impl<S: Storage> Server<S> {
    #[allow(dead_code)]
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    #[allow(dead_code)]
    pub fn into_service(self) -> storage_server::StorageServer<Server<S>> {
        storage_server::StorageServer::new(self)
    }
}

#[tonic::async_trait]
impl<S: Storage> storage_server::Storage for Server<S> {
    type ListBucketsStream =
        impl Stream<Item = std::result::Result<ListBucketsResponse, Status>> + Send;
    type ListObjectsStream =
        impl Stream<Item = std::result::Result<ListObjectsResponse, Status>> + Send;
    type ReadObjectStream =
        impl Stream<Item = std::result::Result<ReadObjectResponse, Status>> + Send;

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
                let w = self
                    .storage
                    .new_sequential_writer(
                        &req.bucket,
                        &req.object,
                        WriteOption::new(req.replica_request, req.replica_chain.to_owned()),
                    )
                    .await?;
                cw = Some(w);
            }
            if let Some(w) = &mut cw {
                w.write_all(&req.content).await?;
            }
        }
        if let Some(mut w) = cw.take() {
            w.close().await?;
        }
        Ok(Response::new(UploadObjectResponse {}))
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let input = request.into_inner();
        self.storage
            .delete_object(&input.bucket, &input.object)
            .await?;
        Ok(Response::new(DeleteObjectResponse {}))
    }

    async fn read_object(
        &self,
        request: Request<ReadObjectRequest>,
    ) -> Result<Response<Self::ReadObjectStream>, Status> {
        let input = request.into_inner();
        let r = self
            .storage
            .new_random_reader(&input.bucket, &input.object)
            .await?;
        let reader = r.to_async_read(input.pos as usize);
        let byte_stream = ReaderStream::new(reader);
        let resp_stream = byte_stream.map(move |res| {
            res.map(|b| ReadObjectResponse {
                content: b.to_vec(),
            })
            .map_err(|e| e.into())
        });
        Ok(Response::new(resp_stream))
    }

    async fn list_buckets(
        &self,
        _request: Request<ListBucketsRequest>,
    ) -> Result<Response<Self::ListBucketsStream>, Status> {
        let buckets = self.storage.list_buckets().await?.batched(10);
        let bucket_names: VecStrStream = buckets.collect().await?.into();
        let stream = bucket_names.map(|name| Ok(ListBucketsResponse { bucket: name }));
        Ok(Response::new(stream))
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<Self::ListObjectsStream>, Status> {
        let input = request.into_inner();
        let objects = self.storage.list_objects(&input.bucket).await?.batched(10);
        let names: VecStrStream = objects.collect().await?.into();
        let stream = names.map(|name| Ok(ListObjectsResponse { object: name }));
        Ok(Response::new(stream))
    }
}

struct VecStrStream {
    s: Vec<String>,
    idx: usize,
}

impl From<Vec<String>> for VecStrStream {
    fn from(s: Vec<String>) -> Self {
        Self { s, idx: 0 }
    }
}

impl Stream for VecStrStream {
    type Item = String;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut me = self.get_mut();
        if me.idx >= me.s.len() {
            return Poll::Ready(None);
        }
        let s = me.s[me.idx].to_owned();
        me.idx += 1;
        Poll::Ready(Some(s))
    }
}
