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
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("invalid response")]
    InvalidResponse,
    #[error("unknown error: {0}")]
    Unknown(String),
}

impl Error {
    pub fn unknown(s: impl ToString) -> Self {
        Self::Unknown(s.to_string())
    }
}

impl From<Status> for Error {
    fn from(s: Status) -> Self {
        match s.code() {
            Code::NotFound => Error::NotFound(s.message().to_owned()),
            Code::AlreadyExists => Error::AlreadyExists(s.message().to_owned()),
            Code::InvalidArgument => Error::InvalidArgument(s.message().to_owned()),
            _ => Error::Unknown(s.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
