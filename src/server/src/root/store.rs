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
        group_request_union::Request::{BatchWrite, CreateShard, Delete, Get, Put},
        CreateShardRequest, GroupRequest, GroupRequestUnion, ShardDesc, *,
    },
    v1::{DeleteRequest, GetRequest, PutRequest},
};

use crate::{
    bootstrap::{ROOT_GROUP_ID, ROOT_SHARD_ID},
    node::replica::Replica,
    Error, Result,
};

pub struct RootStore {
    replica: Arc<Replica>,
}

impl RootStore {
    pub fn new(replica: Arc<Replica>) -> Self {
        Self { replica }
    }

    pub async fn create_shard(&self, shard: ShardDesc) -> Result<u64> {
        let group_id = self.place_shared_group(&shard);
        self.replica
            .execute(&GroupRequest {
                group_id,
                request: Some(GroupRequestUnion {
                    request: Some(CreateShard(CreateShardRequest { shard: Some(shard) })),
                }),
            })
            .await?;
        Ok(group_id)
    }

    pub fn place_shared_group(&self, _shard: &ShardDesc) -> u64 {
        // TODO: pre-alloc group at start and choose freest
        ROOT_GROUP_ID
    }

    pub async fn batch_write(&self, batch: BatchWriteRequest) -> Result<()> {
        self.replica
            .execute(&GroupRequest {
                group_id: ROOT_GROUP_ID,
                request: Some(GroupRequestUnion {
                    request: Some(BatchWrite(batch)),
                }),
            })
            .await?;
        Ok(())
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.replica
            .execute(&GroupRequest {
                group_id: ROOT_GROUP_ID,
                request: Some(GroupRequestUnion {
                    request: Some(Put(ShardPutRequest {
                        shard_id: ROOT_SHARD_ID,
                        put: Some(PutRequest { key, value }),
                    })),
                }),
            })
            .await?;
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let resp = self
            .replica
            .execute(&GroupRequest {
                group_id: ROOT_GROUP_ID,
                request: Some(GroupRequestUnion {
                    request: Some(Get(ShardGetRequest {
                        shard_id: ROOT_SHARD_ID,
                        get: Some(GetRequest {
                            key: key.to_owned(),
                        }),
                    })),
                }),
            })
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

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.replica
            .execute(&GroupRequest {
                group_id: ROOT_GROUP_ID,
                request: Some(GroupRequestUnion {
                    request: Some(Delete(ShardDeleteRequest {
                        shard_id: ROOT_SHARD_ID,
                        delete: Some(DeleteRequest {
                            key: key.to_owned(),
                        }),
                    })),
                }),
            })
            .await?;
        Ok(())
    }

    pub async fn list(&self, _prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        // TODO(zojw): impl scan prefix under database_id + prefix.
        Ok(vec![])
    }
}
