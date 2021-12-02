// Copyright 2021 The Engula Authors.
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

use thiserror::Error;

/// Errors for all journal operations.
#[derive(Error, Debug)]
pub enum Error {
    #[error("{0} is not found")]
    NotFound(String),
    #[error("{0} already exists")]
    AlreadyExists(String),
    #[error("{0}")]
    InvalidArgument(String),
    #[error(transparent)]
    GrpcStatus(#[from] tonic::Status),
    #[error(transparent)]
    GrpcTransport(#[from] tonic::transport::Error),
    #[error(transparent)]
    Serialize(#[from] serde_json::Error),
}

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        let (code, message) = match err {
            Error::NotFound(s) => (tonic::Code::NotFound, s),
            Error::AlreadyExists(s) => (tonic::Code::AlreadyExists, s),
            Error::InvalidArgument(s) => (tonic::Code::InvalidArgument, s),
            Error::GrpcStatus(s) => (s.code(), s.message().to_owned()),
            Error::GrpcTransport(e) => (tonic::Code::Internal, e.to_string()),
            Error::Serialize(s) => (tonic::Code::Internal, s.to_string()),
        };
        tonic::Status::new(code, message)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
