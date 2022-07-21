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

use engula_api::server::v1::{error_detail_union, error_detail_union::Value, GroupResponse};

pub type Result<T> = std::result::Result<T, Error>;
pub type AppResult<T> = std::result::Result<T, AppError>;

#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("{0} not found")]
    NotFound(String),

    #[error("deadline is exceeded")]
    DeadlineExceeded,

    #[error("internal {0}")]
    Internal(Box<dyn StdError + Send + 'static>),
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0} not found")]
    NotFound(String),

    #[error("group response error")]
    GroupResponseError(Option<error_detail_union::Value>),

    #[error("rpc {0}")]
    Rpc(tonic::Status),

    #[error("internal {0}")]
    Internal(Box<dyn StdError + Send + 'static>),
}

impl Error {
    pub fn from_group_response(r: &GroupResponse) -> Option<Self> {
        r.error.as_ref().map(|err| {
            Error::GroupResponseError(
                err.details
                    .get(0)
                    .cloned()
                    .and_then(|d| d.detail)
                    .and_then(|u| u.value),
            )
        })
    }

    pub fn should_retry(&self) -> bool {
        match self {
            Error::Internal(_) => false,
            Error::Rpc(_) => true,
            Error::NotFound(_) => true,
            Error::GroupResponseError(None) => true,
            Error::GroupResponseError(Some(err)) => match err {
                Value::NotLeader(_) => true,
                Value::NotMatch(_) => true,
                Value::ServerIsBusy(_) => false,
                Value::GroupNotFound(_) => true,
                Value::NotRoot(_) => true,
                Value::StatusCode(_) => true,
            },
        }
    }
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        use engula_api::server::v1;
        use prost::Message;
        use tonic::Code;

        match status.code() {
            Code::Ok => panic!("invalid argument"),
            Code::Unknown if !status.details().is_empty() => v1::Error::decode(status.details())
                .map(|err| {
                    Error::GroupResponseError(
                        err.details
                            .get(0)
                            .cloned()
                            .and_then(|d| d.detail)
                            .and_then(|u| u.value),
                    )
                })
                .unwrap_or_else(|_| Error::Rpc(status)),
            _ => Error::Rpc(status),
        }
    }
}

impl From<Error> for AppError {
    fn from(err: Error) -> Self {
        match err {
            Error::Internal(v) => AppError::Internal(v),
            Error::NotFound(v) => AppError::NotFound(v),
            Error::Rpc(_) | Error::GroupResponseError(_) => AppError::DeadlineExceeded,
        }
    }
}
