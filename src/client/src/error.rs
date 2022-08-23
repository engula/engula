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

use std::error::Error as StdError;

use engula_api::server::v1::{GroupDesc, ReplicaDesc, RootDesc};

pub type Result<T> = std::result::Result<T, Error>;
pub type AppResult<T> = std::result::Result<T, AppError>;

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("{0} not found")]
    NotFound(String),

    #[error("{0} already exists")]
    AlreadyExists(String),

    #[error("invalid argument {0}")]
    InvalidArgument(String),

    #[error("deadline exceeded {0}")]
    DeadlineExceeded(String),

    #[error("network: {0}")]
    Network(tonic::Status),

    #[error("internal {0}")]
    Internal(Box<dyn StdError + Send + Sync + 'static>),
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid argument {0}")]
    InvalidArgument(String),

    #[error("deadline exceeded {0}")]
    DeadlineExceeded(String),

    #[error("{0} already exists")]
    AlreadyExists(String),

    #[error("{0} not found")]
    NotFound(String),

    #[error("{0} is exhausted")]
    ResourceExhausted(String),

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

    /// This indicates that the `GroupClient` has not been able to access the group leader after
    /// retries many times.
    #[error("group {0} not accessable")]
    GroupNotAccessable(u64),

    #[error("transport {0}")]
    Transport(tonic::Status),

    #[error("connect {0}")]
    Connect(tonic::Status),

    #[error("rpc {0}")]
    Rpc(tonic::Status),

    #[error("internal {0}")]
    Internal(Box<dyn StdError + Send + Sync + 'static>),
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        use engula_api::server::v1;
        use prost::Message;
        use tonic::Code;

        match status.code() {
            Code::Ok => panic!("invalid argument"),
            Code::InvalidArgument => Error::InvalidArgument(status.message().into()),
            Code::Cancelled if status.message().contains("Timeout expired") => {
                Error::DeadlineExceeded(status.message().into())
            }
            Code::AlreadyExists => Error::AlreadyExists(status.message().into()),
            Code::ResourceExhausted => Error::ResourceExhausted(status.message().into()),
            Code::NotFound => Error::NotFound(status.message().into()),
            Code::Internal => Error::Internal(status.message().into()),
            Code::Unavailable if retryable_rpc_err(&status) => Error::Connect(status),
            Code::Unknown if !status.details().is_empty() => v1::Error::decode(status.details())
                .map(Into::into)
                .unwrap_or_else(|_| Error::Rpc(status)),
            Code::Unknown if transport_err(&status) => Error::Transport(status),
            _ => Error::Rpc(status),
        }
    }
}

impl From<engula_api::server::v1::Error> for Error {
    fn from(err: engula_api::server::v1::Error) -> Self {
        use engula_api::server::v1::error_detail_union::Value;
        use tonic::Status;

        if err.details.is_empty() {
            return Status::internal("ErrorDetails is empty").into();
        }

        // Only convert first error detail.
        let detail = &err.details[0];
        let msg = detail.message.clone();
        match detail.detail.as_ref().and_then(|u| u.value.clone()) {
            Some(Value::GroupNotFound(v)) => Error::GroupNotFound(v.group_id),
            Some(Value::NotLeader(v)) => Error::NotLeader(v.group_id, v.term, v.leader),
            Some(Value::NotRoot(v)) => {
                Error::NotRootLeader(v.root.unwrap_or_default(), v.term, v.leader)
            }
            Some(Value::NotMatch(v)) => Error::EpochNotMatch(v.descriptor.unwrap_or_default()),
            Some(Value::StatusCode(v)) => Status::new(v.into(), msg).into(),
            _ => Status::internal(format!("unknown error detail, msg: {msg}")).into(),
        }
    }
}

impl From<Error> for AppError {
    fn from(err: Error) -> Self {
        match err {
            Error::InvalidArgument(v) => AppError::InvalidArgument(v),
            Error::DeadlineExceeded(v) => AppError::DeadlineExceeded(v),
            Error::NotFound(v) => AppError::NotFound(v),
            Error::AlreadyExists(v) => AppError::AlreadyExists(v),
            Error::Internal(v) => AppError::Internal(v),

            Error::Transport(status) => AppError::Network(status),
            Error::Connect(status) => panic!("do not expose connect error {status:?} to user"),
            Error::Rpc(status) => panic!("unknown error: {status:?}"),

            Error::EpochNotMatch(_)
            | Error::ResourceExhausted(_)
            | Error::GroupNotFound(_)
            | Error::GroupNotAccessable(_)
            | Error::NotRootLeader(..)
            | Error::NotLeader(..) => unreachable!(),
        }
    }
}

pub fn find_io_error(status: &tonic::Status) -> Option<&std::io::Error> {
    use tonic::Code;
    if status.code() == Code::Unavailable || status.code() == Code::Unknown {
        find_source::<std::io::Error>(status)
    } else {
        None
    }
}

pub fn find_source<E: std::error::Error + 'static>(err: &tonic::Status) -> Option<&E> {
    use std::error::Error;
    let mut cause = err.source();
    while let Some(err) = cause {
        if let Some(typed) = err.downcast_ref() {
            return Some(typed);
        }
        cause = err.source();
    }
    None
}

pub fn retryable_io_err(err: &std::io::Error) -> bool {
    use std::io::ErrorKind;

    matches!(err.kind(), ErrorKind::ConnectionRefused)
}

pub fn retryable_rpc_err(status: &tonic::Status) -> bool {
    use tonic::Code;
    if status.code() == Code::Unavailable
        && status
            .message()
            .contains("error trying to connect: deadline has elapsed")
    {
        // connection timeout.
        true
    } else {
        let mut cause = status.source();
        while let Some(err) = cause {
            if let Some(err) = err.downcast_ref::<std::io::Error>() {
                return retryable_io_err(err);
            } else if err
                .to_string()
                .ends_with("operation was canceled: connection closed")
            {
                // The request is dropped in an internal queue, which is guaranteed to have not been
                // sent to the server. See https://github.com/hyperium/hyper/blob/bb3af17ce1a3841e9170adabcce595c7c8743ea7/src/client/dispatch.rs#L209 for details.
                return true;
            }
            cause = err.source();
        }
        false
    }
}

pub fn transport_io_err(err: &std::io::Error) -> bool {
    use std::io::ErrorKind;

    matches!(
        err.kind(),
        ErrorKind::ConnectionReset | ErrorKind::BrokenPipe
    )
}

pub fn transport_err(status: &tonic::Status) -> bool {
    use tonic::Code;
    if status.code() == Code::Unknown && status.message().starts_with("transport error") {
        let mut cause = status.source();
        while let Some(err) = cause {
            if let Some(err) = err.downcast_ref::<std::io::Error>() {
                return transport_io_err(err);
            }
            cause = err.source();
        }
    }
    false
}
