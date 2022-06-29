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

use engula_api::server::v1::{error_detail_union, error_detail_union::Value, GroupResponse};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("transport {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("multi transport errors")]
    MultiTransport(Vec<tonic::transport::Error>),

    #[error("rpc {0}")]
    Rpc(#[from] tonic::Status),

    #[error("{0} not found")]
    NotFound(String),

    #[error("group response error")]
    GroupResponseError(Option<error_detail_union::Value>),
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
            Error::Transport(_) => false,
            Error::MultiTransport(_) => false,
            Error::Rpc(_) => false,
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
