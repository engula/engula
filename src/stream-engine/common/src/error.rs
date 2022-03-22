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

use futures::channel::oneshot;
use thiserror::Error;

pub type IoResult<T> = std::result::Result<T, std::io::Error>;
pub type IoKindResult<T> = std::result::Result<T, std::io::ErrorKind>;

/// Errors for all stream engine operations.
#[derive(Error, Debug)]
pub enum Error {
    #[error("{0} is not found")]
    NotFound(String),
    #[error("not leader, new leader is {0}")]
    NotLeader(String),
    #[error("{0} already exists")]
    AlreadyExists(String),
    #[error("{0}")]
    InvalidArgument(String),
    #[error("invalid response")]
    InvalidResponse,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("{0} is staled")]
    Staled(String),
    #[error("{0}")]
    Corruption(String),
    #[error(transparent)]
    Unknown(Box<dyn std::error::Error + Send>),
}

#[must_use = "this `Result` may be an `Err` variant, which should be handled"]
pub type Result<T> = std::result::Result<T, Error>;

impl From<oneshot::Canceled> for Error {
    fn from(_: oneshot::Canceled) -> Self {
        use std::io;

        // Because we cannot determine whether a canceled proposal acked, it is
        // processed according to the third state of distributed system.
        Error::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "task has been canceled",
        ))
    }
}

impl From<prost::DecodeError> for Error {
    fn from(err: prost::DecodeError) -> Self {
        Error::Corruption(err.to_string())
    }
}

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        match s.code() {
            tonic::Code::NotFound => Error::NotFound(s.message().into()),
            tonic::Code::AlreadyExists => Error::AlreadyExists(s.message().into()),
            tonic::Code::InvalidArgument => Error::InvalidArgument(s.message().into()),
            tonic::Code::FailedPrecondition => Error::Staled(s.message().into()),
            tonic::Code::DataLoss => Error::Corruption(s.message().into()),
            _ => Error::Unknown(Box::new(s)),
        }
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(e: tonic::transport::Error) -> Self {
        Error::Unknown(Box::new(e))
    }
}

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        let (code, message) = match err {
            Error::NotLeader(_) => unreachable!(),
            Error::NotFound(s) => (tonic::Code::NotFound, s),
            Error::AlreadyExists(s) => (tonic::Code::AlreadyExists, s),
            Error::InvalidArgument(s) => (tonic::Code::InvalidArgument, s),
            Error::InvalidResponse => (tonic::Code::InvalidArgument, "invalid response".into()),
            Error::Io(s) => (tonic::Code::Unknown, s.to_string()),
            Error::Unknown(s) => (tonic::Code::Unknown, s.to_string()),
            Error::Staled(s) => (tonic::Code::FailedPrecondition, s),
            Error::Corruption(s) => (tonic::Code::DataLoss, s),
        };
        tonic::Status::new(code, message)
    }
}

impl From<std::io::ErrorKind> for Error {
    fn from(kind: std::io::ErrorKind) -> Self {
        Error::Io(kind.into())
    }
}
