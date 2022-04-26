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

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use engula_engine::{migrate_record, Db};

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

fn run_background(db: Db, exit_flags: Arc<AtomicBool>) {
    while !exit_flags.load(Ordering::Acquire) {
        unsafe {
            //compact_segments(|record_base| migrate_record(db.clone(), record_base));
        }

        std::thread::sleep(Duration::from_millis(100));
    }
}

fn bootstrap_background_service(db: Db) -> (Arc<AtomicBool>, JoinHandle<()>) {
    let exit_flag = Arc::new(AtomicBool::new(false));
    let cloned_exit_flag = exit_flag.clone();
    let join_handle = std::thread::spawn(move || {
        //run_background(db, cloned_exit_flag);
    });
    (exit_flag, join_handle)
}

pub fn run(config: Config) -> Result<()> {
    let db = Db::default();
    let (exit_flag, join_handle) = bootstrap_background_service(db.clone());
    match config.driver_mode {
        DriverMode::Mio => mio::Server::new(db, config)?.run()?,
        #[cfg(target_os = "linux")]
        DriverMode::Uio => uio::Server::new(db, config)?.run()?,
    };
    exit_flag.store(true, Ordering::Release);
    join_handle.join().unwrap();
    Ok(())
}
