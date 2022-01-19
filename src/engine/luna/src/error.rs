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

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0} already exists")]
    AlreadyExists(String),
    #[error("{0}")]
    InvalidArgument(String),
    #[error("corrupted: {0}")]
    Corrupted(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Kernel(#[from] engula_kernel::Error),
    #[error(transparent)]
    Journal(#[from] engula_journal::Error),
    #[error(transparent)]
    Storage(#[from] engula_storage::Error),
    #[error(transparent)]
    Unknown(Box<dyn std::error::Error>),
}

impl Error {
    pub fn unknown(err: impl std::error::Error + 'static) -> Self {
        Self::Unknown(Box::new(err))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
