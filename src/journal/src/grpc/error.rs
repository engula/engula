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
use tonic::{Code, Status};

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    GrpcStatus(#[from] Status),
    #[error(transparent)]
    GrpcTransport(#[from] tonic::transport::Error),
    #[error(transparent)]
    Serialize(#[from] serde_json::Error),
}

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        match err {
            Error::GrpcStatus(s) => s,
            Error::GrpcTransport(e) => Status::new(Code::Internal, e.to_string()),
            Error::Serialize(s) => Status::new(Code::Internal, s.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
