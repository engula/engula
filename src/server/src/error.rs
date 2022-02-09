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

use thiserror::Error;
use tonic::{Code, Status};

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0} is not found")]
    NotFound(String),
    #[error("{0} already exists")]
    AlreadyExists(String),
    #[error("invalid request")]
    InvalidRequest,
}

impl From<Error> for Status {
    fn from(err: Error) -> Status {
        let (code, message) = match err {
            Error::NotFound(m) => (Code::NotFound, m),
            Error::AlreadyExists(m) => (Code::AlreadyExists, m),
            Error::InvalidRequest => (Code::InvalidArgument, "invalid request".to_owned()),
        };
        Status::new(code, message)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
