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

use tonic::{Request, Response, Status};

use super::proto::*;
use crate::{Object, ObjectUploader, Storage};

pub struct Server<O, S>
where
    O: Object,
    S: Storage<O>,
{
    storage: S,
    _object: PhantomData<O>,
}

impl<O, S> Server<O, S>
where
    O: Object + Send + Sync + 'static,
    O::Error: Send + Sync + 'static,
    S: Storage<O> + Send + Sync + 'static,
    S::ObjectUploader: Send + Sync + 'static,
    Status: From<O::Error>,
{
    pub fn new(storage: S) -> Self {
        Server {
            storage,
            _object: PhantomData,
        }
    }

    pub fn into_service(self) -> storage_server::StorageServer<Server<O, S>> {
        storage_server::StorageServer::new(self)
    }
}

#[tonic::async_trait]
impl<O, S> storage_server::Storage for Server<O, S>
where
    O: Object + Send + Sync + 'static,
    O::Error: Send + Sync + 'static,
    S: Storage<O> + Send + Sync + 'static,
    S::ObjectUploader: Send + Sync + 'static,
    Status: From<O::Error>,
{
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
        request: Request<UploadObjectRequest>,
    ) -> Result<Response<UploadObjectResponse>, Status> {
        let input = request.into_inner();
        let mut up = self
            .storage
            .upload_object(&input.bucket, &input.object)
            .await?;
        up.write(&input.content).await?;
        up.finish().await?;
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
    ) -> Result<Response<ReadObjectResponse>, Status> {
        let input = request.into_inner();
        let object = self.storage.object(&input.bucket, &input.object).await?;
        let mut buf = vec![0; input.length as usize];
        let len = object.read_at(&mut buf, input.offset as usize).await?;
        let output = ReadObjectResponse {
            content: buf[0..len].to_owned(),
        };
        Ok(Response::new(output))
    }
}
