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

use std::fmt::{self, Debug};

use aws_sdk_s3::SdkError;
use thiserror::Error;

/// Errors for all storage operations.
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

pub type Result<T> = std::result::Result<T, Error>;

impl<E, R> From<SdkError<E, R>> for Error
where
    R: Debug,
    E: std::error::Error,
{
    fn from(e: SdkError<E, R>) -> Self {
        let e = Box::new(StrError::new(e.to_string()));
        Self::Unknown(e)
    }
}

#[derive(Debug)]
struct StrError {
    details: String,
}

impl StrError {
    fn new(msg: String) -> Self {
        Self { details: msg }
    }
}

impl fmt::Display for StrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl std::error::Error for StrError {
    fn description(&self) -> &str {
        &self.details
    }
}
