// Copyright 2022 The Engula Authors.
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

use std::sync::Arc;

use engula_api::{
    server::v1::{
        group_request_union::Request::{self, *},
        GroupRequest, GroupRequestUnion, *,
    },
    v1::{DeleteRequest, GetRequest, PutRequest},
};

use crate::{bootstrap::ROOT_GROUP_ID, node::replica::Replica, Error, Result};

pub struct RootStore {
    replica: Arc<Replica>,
}

impl RootStore {
    pub fn new(replica: Arc<Replica>) -> Self {
        Self { replica }
    }

    pub async fn batch_write(&self, batch: BatchWriteRequest) -> Result<()> {
        self.submit_request(BatchWrite(batch)).await?;
        Ok(())
    }

    pub async fn put(&self, shard_id: u64, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.submit_request(Put(ShardPutRequest {
            shard_id,
            put: Some(PutRequest { key, value }),
        }))
        .await?;
        Ok(())
    }

    pub async fn get(&self, shard_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let resp = self
            .submit_request(Get(ShardGetRequest {
                shard_id,
                get: Some(GetRequest {
                    key: key.to_owned(),
                }),
            }))
            .await?;
        let resp = resp
            .response
            .ok_or_else(|| Error::InvalidArgument("GetResponse".into()))?
            .response
            .ok_or_else(|| Error::InvalidArgument("GetResponseUnion".into()))?;
        if let group_response_union::Response::Get(resp) = resp {
            Ok(resp.value)
        } else {
            Err(Error::InvalidArgument("GetResponse".into()))
        }
    }

    pub async fn delete(&self, shard_id: u64, key: &[u8]) -> Result<()> {
        self.submit_request(Delete(ShardDeleteRequest {
            shard_id,
            delete: Some(DeleteRequest {
                key: key.to_owned(),
            }),
        }))
        .await?;
        Ok(())
    }

    pub async fn list(&self, shard_id: u64, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let resp = self
            .submit_request(PrefixList(ShardPrefixListRequest {
                shard_id,
                prefix: prefix.to_owned(),
            }))
            .await?;
        let resp = resp
            .response
            .ok_or_else(|| Error::InvalidArgument("PrefixListResponse".into()))?
            .response
            .ok_or_else(|| Error::InvalidArgument("PrefixListUnionResponse".into()))?;

        if let group_response_union::Response::PrefixList(resp) = resp {
            Ok(resp.values)
        } else {
            Err(Error::InvalidArgument("PrefixListResponse".into()))
        }
    }

    async fn submit_request(&self, req: Request) -> Result<GroupResponse> {
        use crate::node::replica::{retry::execute, ExecCtx};

        let epoch = self.replica.epoch();
        let request = GroupRequest {
            group_id: ROOT_GROUP_ID,
            epoch,
            request: Some(GroupRequestUnion { request: Some(req) }),
        };

        execute(&self.replica, ExecCtx::default(), request).await
    }
}
