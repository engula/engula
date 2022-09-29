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
use engula_api::server::v1::{GroupDesc, ReplicaDesc, RootDesc};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    // business errors
    #[error("{0} already exists")]
    AlreadyExists(String),

    #[error("invalid argument {0}")]
    InvalidArgument(String),

    #[error("deadline exceeded {0}")]
    DeadlineExceeded(String),

    #[error("database {0} not found")]
    DatabaseNotFound(String),

    #[error("no available group")]
    NoAvaliableGroup,

    #[error("{0} is exhausted")]
    ResourceExhausted(String),

    // internal errors
    #[error("shard {0} not found")]
    ShardNotFound(u64),

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

    #[error("io {0}")]
    Io(#[from] std::io::Error),

    #[error("rocksdb {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("rpc {0}")]
    Rpc(tonic::Status),

    // retryable errors
    #[error("group {0} not ready")]
    GroupNotReady(u64),

    #[error("service is busy: {0}")]
    ServiceIsBusy(BusyReason),

    #[error("forward request to dest group")]
    Forward(crate::node::migrate::ForwardCtx),

    #[error("group epoch not match")]
    EpochNotMatch(GroupDesc),

    #[error("group {0} not found")]
    GroupNotFound(u64),

    #[error("not root leader")]
    NotRootLeader(RootDesc, u64, Option<ReplicaDesc>),

    #[error("not leader of group {0}")]
    NotLeader(
        /* group_id */ u64,
        /* term */ u64,
        Option<ReplicaDesc>,
    ),

    #[error("abort schedule task, {0}")]
    AbortScheduleTask(&'static str),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum BusyReason {
    Transfering,
    Migrating,
    AclGuard,
    PendingConfigChange,
    RequestChannelFulled,
    ProposalDropped,
}

impl std::fmt::Display for BusyReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reason = match self {
            BusyReason::AclGuard => "take acl guard",
            BusyReason::Migrating => "in shard migrating",
            BusyReason::PendingConfigChange => "has pending config change",
            BusyReason::Transfering => "leader transfering",
            BusyReason::RequestChannelFulled => "request channel fulled",
            BusyReason::ProposalDropped => "proposal dropped by raft",
        };
        f.write_str(reason)
    }
}

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        use engula_api::server::v1;
        use prost::Message;
        use tonic::{Code, Status};

        match e {
            Error::InvalidArgument(msg) => Status::invalid_argument(msg),
            Error::DeadlineExceeded(msg) => Status::deadline_exceeded(msg),
            err @ Error::DatabaseNotFound(_) => Status::not_found(err.to_string()),
            err @ Error::AlreadyExists(_) => Status::already_exists(err.to_string()),
            Error::ResourceExhausted(msg) => Status::resource_exhausted(msg),

            Error::GroupNotFound(group_id) => Status::with_details(
                Code::Unknown,
                e.to_string(),
                v1::Error::group_not_found(group_id).encode_to_vec().into(),
            ),
            Error::NotLeader(group_id, term, leader) => Status::with_details(
                Code::Unknown,
                format!("not leader of group {}", group_id),
                v1::Error::not_leader(group_id, term, leader)
                    .encode_to_vec()
                    .into(),
            ),
            Error::NotRootLeader(root, term, leader) => Status::with_details(
                Code::Unknown,
                "not root",
                v1::Error::not_root_leader(root, term, leader)
                    .encode_to_vec()
                    .into(),
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
            | Error::AbortScheduleTask(_)
            | Error::ClusterNotMatch
            | Error::InvalidData(_)
            | Error::Transport(_)
            | Error::Io(_)
            | Error::RocksDb(_)
            | Error::Raft(_)
            | Error::RaftEngine(_)
            | Error::ShardNotFound(_)
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
        engula_client::Error::from(status).into()
    }
}

impl From<Error> for engula_api::server::v1::Error {
    fn from(err: Error) -> Self {
        use engula_api::server::v1;
        use tonic::Code;

        match err {
            Error::GroupNotFound(group_id) => v1::Error::group_not_found(group_id),
            Error::NotLeader(group_id, term, leader) => {
                v1::Error::not_leader(group_id, term, leader)
            }
            Error::NotRootLeader(root, term, leader) => {
                v1::Error::not_root_leader(root, term, leader)
            }
            Error::EpochNotMatch(desc) => v1::Error::not_match(desc),

            Error::InvalidArgument(msg) => v1::Error::status(Code::InvalidArgument.into(), msg),
            Error::DeadlineExceeded(msg) => v1::Error::status(Code::DeadlineExceeded.into(), msg),

            Error::Forward(_) => panic!("Forward only used inside node"),
            Error::ServiceIsBusy(_) => panic!("ServiceIsBusy only used inside node"),
            Error::GroupNotReady(_) => panic!("GroupNotReady only used inside node"),
            Error::AbortScheduleTask(_) => panic!("AbortScheduleTask only used inside node"),
            Error::AlreadyExists(msg) => v1::Error::status(Code::AlreadyExists.into(), msg),

            err @ (Error::Transport(_)
            | Error::ResourceExhausted(_)
            | Error::Raft(_)
            | Error::RaftEngine(_)
            | Error::RocksDb(_)
            | Error::Io(_)
            | Error::InvalidData(_)
            | Error::DatabaseNotFound(_)
            | Error::ShardNotFound(_)
            | Error::ClusterNotMatch
            | Error::NoAvaliableGroup
            | Error::Canceled
            | Error::Rpc(_)) => v1::Error::status(Code::Internal.into(), err.to_string()),
        }
    }
}

impl From<engula_api::server::v1::Error> for Error {
    fn from(err: engula_api::server::v1::Error) -> Self {
        engula_client::Error::from(err).into()
    }
}

impl From<engula_client::Error> for Error {
    fn from(err: engula_client::Error) -> Self {
        match err {
            engula_client::Error::InvalidArgument(v) => Error::InvalidArgument(v),
            engula_client::Error::DeadlineExceeded(v) => Error::DeadlineExceeded(v),
            engula_client::Error::AlreadyExists(v) => Error::AlreadyExists(v),
            engula_client::Error::ResourceExhausted(v) => Error::ResourceExhausted(v),
            engula_client::Error::Rpc(err) => Error::Rpc(err),
            engula_client::Error::Connect(err) => Error::Rpc(err),
            engula_client::Error::Transport(err) => Error::Rpc(err),

            engula_client::Error::GroupNotFound(v) => Error::GroupNotFound(v),
            engula_client::Error::NotRootLeader(desc, term, leader) => {
                Error::NotRootLeader(desc, term, leader)
            }
            engula_client::Error::NotLeader(group, term, leader) => {
                Error::NotLeader(group, term, leader)
            }
            engula_client::Error::EpochNotMatch(v) => Error::EpochNotMatch(v),

            // NOTE: This is a fallback, for some scenarios where you don't need to deal with
            // `GroupNotAccessable` raised by `GroupClient`. (`GroupNotReady` only used inside
            // nodes)
            engula_client::Error::GroupNotAccessable(id) => Error::GroupNotReady(id),

            // FIXME(walter) handle unknown errors.
            engula_client::Error::NotFound(v) => panic!("unknown not found: {v}"),
            engula_client::Error::Internal(v) => panic!("internal error: {v:?}"),
        }
    }
}
