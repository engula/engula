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

use std::{net::TcpListener, panic, thread};

use tracing::info;

use super::worker;
use crate::{Config, Db, Result};

pub struct Server {
    config: Config,
    workers: Vec<thread::JoinHandle<()>>,
}

impl Server {
    pub fn new(config: Config) -> Server {
        Self {
            config,
            workers: Vec::new(),
        }
    }

    pub fn run(mut self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.addr)?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        info!("Server is running at {}", addr);

        let db = Db::default();
        for i in 0..self.config.num_threads {
            let listener = listener.try_clone()?;
            let db = db.clone();
            let handle = thread::Builder::new()
                .name(format!("engula-worker-{}", i))
                .spawn(move || {
                    worker::run(listener, db);
                })?;
            self.workers.push(handle);
        }

        for handle in self.workers {
            if let Err(err) = handle.join() {
                panic::resume_unwind(err);
            }
        }

        Ok(())
    }
}
