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

use engula_journal::Error as JournalError;
use engula_storage::Error as StorageError;
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
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Unknown(Box<dyn std::error::Error + Send>),
}

impl Error {
    pub fn unknown(err: impl std::error::Error + Send + 'static) -> Self {
        Self::Unknown(Box::new(err))
    }
}

impl From<JournalError> for Error {
    fn from(err: JournalError) -> Self {
        match err {
            JournalError::NotFound(s) => Self::NotFound(s),
            JournalError::AlreadyExists(s) => Self::AlreadyExists(s),
            JournalError::InvalidArgument(s) => Self::InvalidArgument(s),
        }
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::NotFound(s) => Self::NotFound(s),
            StorageError::AlreadyExists(s) => Self::AlreadyExists(s),
            StorageError::InvalidArgument(s) => Self::InvalidArgument(s),
            StorageError::Io(err) => Self::Io(err),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
