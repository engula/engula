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

use std::io;

use thiserror::Error;

use crate::{FrameError, ParseError};

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error("unknown error: {0}")]
    Unknown(String),
}

impl From<&str> for Error {
    fn from(s: &str) -> Error {
        Error::Unknown(s.to_owned())
    }
}

impl From<String> for Error {
    fn from(s: String) -> Error {
        Error::Unknown(s)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
