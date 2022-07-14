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
use engula_api::server::v1::{GroupDesc, ReplicaDesc};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    // business errors
    #[error("invalid argument {0}")]
    InvalidArgument(String),

    #[error("deadline exceeded {0}")]
    DeadlineExceeded(String),

    #[error("database {0} not found")]
    DatabaseNotFound(String),

    #[error("database {0} exist")]
    DatabaseExist(String),

    #[error("no avaiable group")]
    NoAvaliableGroup,

    #[error("collection {0} exist")]
    CollectionExist(String),

    // internal errors
    #[error("invalid {0} data")]
    InvalidData(String),

    #[error("request canceled")]
    Canceled,

    #[error("cluster not match")]
    ClusterNotMatch,

    #[error("raft {0}")]
    Raft(#[from] raft::Error),

    #[error("raft engine {0}")]
    RaftEngine(#[from] raft_engine::Error),

    #[error("transport {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("multi transport errors")]
    MultiTransport(Vec<tonic::transport::Error>),

    #[error("io {0}")]
    Io(#[from] std::io::Error),

    #[error("rocksdb {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("rpc {0}")]
    Rpc(tonic::Status),

    // retryable errors
    #[error("group {0} not ready")]
    GroupNotReady(u64),

    #[error("service {0} is busy")]
    ServiceIsBusy(&'static str),

    #[error("forward request to dest group")]
    Forward(crate::node::migrate::ForwardCtx),

    #[error("group epoch not match")]
    EpochNotMatch(GroupDesc),

    #[error("group {0} not found")]
    GroupNotFound(u64),

    #[error("not root leader")]
    NotRootLeader(Vec<String>),

    #[error("not leader of group {0}")]
    NotLeader(u64, Option<ReplicaDesc>),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        use engula_api::server::v1;
        use prost::Message;
        use tonic::{Code, Status};

        match e {
            Error::InvalidArgument(msg) => Status::invalid_argument(msg),
            Error::DeadlineExceeded(msg) => Status::deadline_exceeded(msg),
            err @ Error::DatabaseNotFound(_) => Status::not_found(err.to_string()),
            err @ (Error::DatabaseExist(_) | Error::CollectionExist(_)) => {
                Status::already_exists(err.to_string())
            }

            Error::GroupNotFound(group_id) => Status::with_details(
                Code::Unknown,
                e.to_string(),
                v1::Error::group_not_found(group_id).encode_to_vec().into(),
            ),
            Error::NotLeader(group_id, leader) => Status::with_details(
                Code::Unknown,
                format!("not leader of group {}", group_id),
                v1::Error::not_leader(group_id, leader)
                    .encode_to_vec()
                    .into(),
            ),
            Error::NotRootLeader(roots) => Status::with_details(
                Code::Unknown,
                "not root",
                v1::Error::not_root_leader(roots).encode_to_vec().into(),
            ),
            Error::EpochNotMatch(desc) => Status::with_details(
                Code::Unknown,
                "epoch not match",
                v1::Error::not_match(desc).encode_to_vec().into(),
            ),

            Error::Forward(_) => panic!("Forward only used inside node"),
            Error::ServiceIsBusy(_) => panic!("ServiceIsBusy only used inside node"),
            Error::GroupNotReady(_) => panic!("GroupNotReady only used inside node"),

            err @ (Error::Canceled
            | Error::ClusterNotMatch
            | Error::InvalidData(_)
            | Error::Transport(_)
            | Error::MultiTransport(_)
            | Error::Io(_)
            | Error::RocksDb(_)
            | Error::Raft(_)
            | Error::RaftEngine(_)
            | Error::NoAvaliableGroup
            | Error::Rpc(_)) => Status::internal(err.to_string()),
        }
    }
}

impl From<futures::channel::oneshot::Canceled> for Error {
    fn from(_: futures::channel::oneshot::Canceled) -> Self {
        Error::Canceled
    }
}

impl From<prost::DecodeError> for Error {
    fn from(err: prost::DecodeError) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        use engula_api::server::v1;
        use prost::Message;
        use tonic::Code;

        match status.code() {
            Code::Ok => panic!("invalid argument"),
            Code::Cancelled => Error::Canceled,
            Code::InvalidArgument => Error::InvalidArgument(status.message().into()),
            Code::DeadlineExceeded => Error::DeadlineExceeded(status.message().into()),
            Code::Unknown if !status.details().is_empty() => v1::Error::decode(status.details())
                .map(Into::into)
                .unwrap_or_else(|_| Error::Rpc(status)),
            _ => Error::Rpc(status),
        }
    }
}

impl From<Error> for engula_api::server::v1::Error {
    fn from(err: Error) -> Self {
        use engula_api::server::v1;
        use tonic::Code;

        match err {
            Error::GroupNotFound(group_id) => v1::Error::group_not_found(group_id),
            Error::NotLeader(group_id, leader) => v1::Error::not_leader(group_id, leader),
            Error::NotRootLeader(roots) => v1::Error::not_root_leader(roots),
            Error::EpochNotMatch(desc) => v1::Error::not_match(desc),

            Error::InvalidArgument(msg) => v1::Error::status(Code::InvalidArgument.into(), msg),
            Error::DeadlineExceeded(msg) => v1::Error::status(Code::DeadlineExceeded.into(), msg),

            Error::Forward(_) => panic!("Forward only used inside node"),
            Error::ServiceIsBusy(_) => panic!("ServiceIsBusy only used inside node"),
            Error::GroupNotReady(_) => panic!("GroupNotReady only used inside node"),

            err @ (Error::Transport(_)
            | Error::MultiTransport(_)
            | Error::Raft(_)
            | Error::RaftEngine(_)
            | Error::RocksDb(_)
            | Error::Io(_)
            | Error::InvalidData(_)
            | Error::DatabaseNotFound(_)
            | Error::DatabaseExist(_)
            | Error::CollectionExist(_)
            | Error::ClusterNotMatch
            | Error::NoAvaliableGroup
            | Error::Canceled
            | Error::Rpc(_)) => v1::Error::status(Code::Internal.into(), err.to_string()),
        }
    }
}

impl From<engula_api::server::v1::Error> for Error {
    fn from(err: engula_api::server::v1::Error) -> Self {
        use engula_api::server::v1::error_detail_union::Value;
        use tonic::Status;

        if err.details.is_empty() {
            return Error::InvalidData("ErrorDetails is empty".into());
        }

        // Only convert first error detail.
        let detail = &err.details[0];
        let msg = detail.message.clone();
        match detail.detail.as_ref().and_then(|u| u.value.clone()) {
            Some(Value::GroupNotFound(v)) => Error::GroupNotFound(v.group_id),
            Some(Value::NotLeader(v)) => Error::NotLeader(v.group_id, v.leader),
            Some(Value::NotRoot(v)) => Error::NotRootLeader(v.root),
            Some(Value::NotMatch(v)) => Error::EpochNotMatch(v.descriptor.unwrap_or_default()),
            Some(Value::StatusCode(v)) => Status::new(v.into(), msg).into(),
            _ => Error::InvalidData(msg),
        }
    }
}

impl From<engula_client::Error> for Error {
    fn from(err: engula_client::Error) -> Self {
        match err {
            engula_client::Error::Transport(err) => Error::Transport(err),
            engula_client::Error::MultiTransport(err) => Error::MultiTransport(err),
            engula_client::Error::Rpc(err) => Error::Rpc(err),
            _ => unreachable!(),
        }
    }
}
