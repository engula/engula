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

use std::io::{Error as IoError, ErrorKind};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0} is not found")]
    NotFound(String),
    #[error("{0} already exists")]
    AlreadyExists(String),
    #[error("{0}")]
    InvalidArgument(String),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for Error {
    fn from(err: IoError) -> Self {
        match err.kind() {
            ErrorKind::NotFound => Self::NotFound(err.to_string()),
            ErrorKind::AlreadyExists => Self::AlreadyExists(err.to_string()),
            ErrorKind::InvalidInput => Self::InvalidArgument(err.to_string()),
            _ => Self::Io(err),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
