use engula_api::server::v1::ReplicaDesc;

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

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid argument {0}")]
    InvalidArgument(String),

    #[error("group {0} not found")]
    GroupNotFound(u64),

    #[error("database {0} not found")]
    DatabaseNotFound(String),

    #[error("invalid {0} data")]
    InvalidData(String),

    #[error("not root leader")]
    NotRootLeader,

    #[error("cluster not match")]
    ClusterNotMatch,

    #[error("not leader of group {0}")]
    NotLeader(u64, Option<ReplicaDesc>),

    #[error("transport {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("io {0}")]
    Io(#[from] std::io::Error),

    #[error("rocksdb {0}")]
    RocksDb(#[from] rocksdb::Error),
}
pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        use engula_api::server::v1;
        use prost::Message;
        use tonic::{Code, Status};

        match e {
            Error::InvalidArgument(msg) => Status::invalid_argument(msg),
            Error::GroupNotFound(group_id) => Status::with_details(
                Code::Unknown,
                e.to_string(),
                v1::Error::group_not_found(group_id).encode_to_vec().into(),
            ),
            err @ Error::DatabaseNotFound(_) => Status::internal(err.to_string()),
            err @ Error::NotRootLeader => Status::internal(err.to_string()),
            err @ Error::InvalidData(_) => Status::internal(err.to_string()),
            err @ Error::ClusterNotMatch => Status::internal(err.to_string()),
            Error::NotLeader(group_id, leader) => Status::with_details(
                Code::Unknown,
                format!("not leader of group {}", group_id),
                v1::Error::not_leader(group_id, leader)
                    .encode_to_vec()
                    .into(),
            ),
            Error::Transport(inner) => Status::internal(inner.to_string()),
            Error::Io(inner) => inner.into(),
            Error::RocksDb(inner) => Status::internal(inner.to_string()),
        }
    }
}

impl From<futures::channel::oneshot::Canceled> for Error {
    fn from(_: futures::channel::oneshot::Canceled) -> Self {
        todo!()
    }
}
