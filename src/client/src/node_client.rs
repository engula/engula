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

use engula_api::{server::v1::*, v1::*};
use tonic::transport::Channel;

#[derive(Debug, Clone)]
pub struct Client {
    client: node_client::NodeClient<Channel>,
}

impl Client {
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let addr = format!("http://{}", addr);
        let client = node_client::NodeClient::connect(addr).await?;
        Ok(Self { client })
    }

    pub async fn get_root(&self) -> Result<Vec<String>, tonic::Status> {
        let mut client = self.client.clone();
        let req = GetRootRequest::default();
        let res = client.get_root(req).await?;
        Ok(res.into_inner().addrs)
    }

    // NOTE: This method is always called by the root group.
    pub async fn create_replica(
        &self,
        replica_id: u64,
        group_desc: GroupDesc,
    ) -> Result<(), tonic::Status> {
        let mut client = self.client.clone();
        let req = CreateReplicaRequest {
            replica_id,
            group: Some(group_desc),
        };
        client.create_replica(req).await?;
        Ok(())
    }

    pub async fn batch_group_requests(
        &self,
        req: BatchRequest,
    ) -> Result<Vec<GroupResponse>, tonic::Status> {
        let mut client = self.client.clone();
        let res = client.batch(req).await?;
        Ok(res.into_inner().responses)
    }

    pub async fn root_heartbeat(
        &self,
        req: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, tonic::Status> {
        let mut client = self.client.clone();
        let res = client.root_heartbeat(req).await?;
        Ok(res.into_inner())
    }
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct RequestBatchBuilder {
    node_id: u64,
    requests: Vec<GroupRequest>,
}

#[allow(unused)]
impl RequestBatchBuilder {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            requests: vec![],
        }
    }

    pub fn get(mut self, group_id: u64, shard_id: u64, key: Vec<u8>) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            shard_id,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::Get(GetRequest { key })),
            }),
        });
        self
    }

    pub fn put(mut self, group_id: u64, shard_id: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            shard_id,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::Put(PutRequest { key, value })),
            }),
        });
        self
    }

    pub fn delete(mut self, group_id: u64, shard_id: u64, key: Vec<u8>) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            shard_id,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::Delete(DeleteRequest { key })),
            }),
        });
        self
    }

    pub fn create_shard(mut self, group_id: u64, shard_desc: ShardDesc) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            shard_id: 0, // create_shard doesn't take this field
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::CreateShard(
                    CreateShardRequest {
                        shard: Some(shard_desc),
                    },
                )),
            }),
        });
        self
    }

    pub fn add_replica(mut self, group_id: u64, replica_id: u64, node_id: u64) -> Self {
        let change_replicas = ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas {
                changes: vec![ChangeReplica {
                    change_type: ChangeReplicaType::Add.into(),
                    replica_id,
                    node_id,
                }],
            }),
        };

        self.requests.push(GroupRequest {
            group_id,
            shard_id: 0, // add_replica doesn't take this field.
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::ChangeReplicas(
                    change_replicas,
                )),
            }),
        });
        self
    }

    pub fn build(self) -> BatchRequest {
        BatchRequest {
            node_id: self.node_id,
            requests: self.requests,
        }
    }
}
