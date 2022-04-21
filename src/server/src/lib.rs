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

#![feature(get_mut_unchecked)]

extern crate core;

use engula_engine::Db;

mod error;
pub use error::{Error, Result};

mod config;
pub use config::{Config, ConfigBuilder, DriverMode};

mod buffer;
pub use buffer::{ReadBuf, WriteBuf};

#[allow(dead_code)]
mod cmd;

#[allow(dead_code)]
mod frame;
use frame::{Error as FrameError, Frame};

mod parse;
use parse::{Parse, ParseError};

mod mio;

#[cfg(target_os = "linux")]
mod uio;

mod io_vec;
use io_vec::{IoVec, Pool};

pub fn run(config: Config) -> Result<()> {
    match config.driver_mode {
        DriverMode::Mio => mio::Server::new(config)?.run(),
        #[cfg(target_os = "linux")]
        DriverMode::Uio => uio::Server::new(config)?.run(),
    }
}
