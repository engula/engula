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

use std::time::Duration;

use engula_api::{server::v1::*, v1::*};
use prost::Message;
use tonic::{transport::Channel, IntoRequest};

#[derive(Debug, Clone)]
pub struct Client {
    client: node_client::NodeClient<Channel>,
}

impl Client {
    pub fn new(channel: Channel) -> Self {
        Client {
            client: node_client::NodeClient::new(channel),
        }
    }

    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let addr = format!("http://{}", addr);
        let client = node_client::NodeClient::connect(addr).await?;
        Ok(Self { client })
    }

    pub async fn get_root(&self) -> Result<RootDesc, tonic::Status> {
        let mut client = self.client.clone();
        let req = GetRootRequest::default();
        let res = client.get_root(req).await?;
        Ok(res.into_inner().root.unwrap_or_default())
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

    // NOTE: This method is always called by the root group.
    pub async fn remove_replica(
        &self,
        replica_id: u64,
        group: GroupDesc,
    ) -> Result<(), tonic::Status> {
        let mut client = self.client.clone();
        let req = RemoveReplicaRequest {
            replica_id,
            group: Some(group),
        };
        client.remove_replica(req).await?;
        Ok(())
    }

    pub async fn batch_group_requests(
        &self,
        req: impl IntoRequest<BatchRequest>,
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

    pub async fn pull(
        &self,
        req: PullRequest,
    ) -> Result<tonic::Streaming<ShardChunk>, tonic::Status> {
        let mut client = self.client.clone();
        let res = client.pull(req).await?;
        Ok(res.into_inner())
    }

    pub async fn forward(&self, req: ForwardRequest) -> Result<ForwardResponse, tonic::Status> {
        let mut client = self.client.clone();
        let res = client.forward(req).await?;
        Ok(res.into_inner())
    }

    pub async fn migrate(&self, req: MigrateRequest) -> Result<MigrateResponse, tonic::Status> {
        let mut client = self.client.clone();
        let res = client.migrate(req).await?;
        Ok(res.into_inner())
    }
}

#[derive(Debug, Clone)]
pub struct RequestBatchBuilder {
    node_id: u64,
    requests: Vec<GroupRequest>,
}

impl RequestBatchBuilder {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            requests: vec![],
        }
    }

    pub fn get(mut self, group_id: u64, epoch: u64, shard_id: u64, key: Vec<u8>) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::Get(ShardGetRequest {
                    shard_id,
                    get: Some(GetRequest { key }),
                })),
            }),
        });
        self
    }

    pub fn put(
        mut self,
        group_id: u64,
        epoch: u64,
        shard_id: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::Put(ShardPutRequest {
                    shard_id,
                    put: Some(PutRequest { key, value }),
                })),
            }),
        });
        self
    }

    pub fn delete(mut self, group_id: u64, epoch: u64, shard_id: u64, key: Vec<u8>) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::Delete(ShardDeleteRequest {
                    shard_id,
                    delete: Some(DeleteRequest { key }),
                })),
            }),
        });
        self
    }

    pub fn create_shard(mut self, group_id: u64, epoch: u64, shard_desc: ShardDesc) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
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

    pub fn add_replica(mut self, group_id: u64, epoch: u64, replica_id: u64, node_id: u64) -> Self {
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
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::ChangeReplicas(
                    change_replicas,
                )),
            }),
        });
        self
    }

    pub fn add_learner(mut self, group_id: u64, epoch: u64, replica_id: u64, node_id: u64) -> Self {
        let change_replicas = ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas {
                changes: vec![ChangeReplica {
                    change_type: ChangeReplicaType::AddLearner.into(),
                    replica_id,
                    node_id,
                }],
            }),
        };

        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::ChangeReplicas(
                    change_replicas,
                )),
            }),
        });
        self
    }

    pub fn remove_replica(mut self, group_id: u64, epoch: u64, replica_id: u64) -> Self {
        let change_replicas = ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas {
                changes: vec![ChangeReplica {
                    change_type: ChangeReplicaType::Remove.into(),
                    replica_id,
                    ..Default::default()
                }],
            }),
        };

        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::ChangeReplicas(
                    change_replicas,
                )),
            }),
        });
        self
    }

    pub fn accept_shard(
        mut self,
        group_id: u64,
        epoch: u64,
        src_group_id: u64,
        src_group_epoch: u64,
        shard_desc: &ShardDesc,
    ) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::AcceptShard(
                    AcceptShardRequest {
                        src_group_id,
                        src_group_epoch,
                        shard_desc: Some(shard_desc.to_owned()),
                    },
                )),
            }),
        });
        self
    }

    pub fn move_replica(
        mut self,
        group_id: u64,
        epoch: u64,
        incoming_voters: Vec<ReplicaDesc>,
        outgoing_voters: Vec<ReplicaDesc>,
    ) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::MoveReplicas(
                    MoveReplicasRequest {
                        incoming_voters,
                        outgoing_voters,
                    },
                )),
            }),
        });
        self
    }

    pub fn transfer_leader(mut self, group_id: u64, epoch: u64, transferee: u64) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::Transfer(TransferRequest {
                    transferee,
                })),
            }),
        });
        self
    }

    pub fn shard_prefix(mut self, group_id: u64, epoch: u64, shard_id: u64, prefix: &[u8]) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::PrefixList(
                    ShardPrefixListRequest {
                        shard_id,
                        prefix: prefix.to_owned(),
                    },
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

#[derive(Default, Clone, Debug)]
pub struct RpcTimeout<T: Message> {
    timeout: Option<Duration>,
    msg: T,
}

impl<T: Message> RpcTimeout<T> {
    pub fn new(timeout: Option<Duration>, msg: T) -> Self {
        RpcTimeout { timeout, msg }
    }
}

impl<T: Message> IntoRequest<T> for RpcTimeout<T> {
    fn into_request(self) -> tonic::Request<T> {
        use tonic::Request;

        let mut req = Request::new(self.msg);
        if let Some(duration) = self.timeout {
            req.set_timeout(duration);
        }
        req
    }
}
