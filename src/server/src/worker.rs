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

use std::net::TcpListener;

use tokio::task;
use tracing::{error, info};

use crate::{Db, Session};

#[derive(Clone)]
struct Worker {
    db: Db,
}

impl Worker {
    fn new(db: Db) -> Worker {
        Self { db }
    }

    async fn run(&mut self, listener: TcpListener) {
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        loop {
            let stream = match listener.accept().await {
                Ok((stream, addr)) => {
                    info!(%addr, "new connection");
                    stream
                }
                Err(err) => {
                    error!(%err, "accept");
                    continue;
                }
            };
            let mut session = Session::new(stream, self.db.clone());
            task::spawn_local(async move {
                if let Err(err) = session.run().await {
                    error!(%err, "session");
                }
            });
        }
    }
}

#[tokio::main(flavor = "current_thread")]
pub async fn run(listener: TcpListener, db: Db) {
    let local = task::LocalSet::new();
    local
        .run_until(async move {
            let mut worker = Worker::new(db);
            worker.run(listener).await;
        })
        .await;
}
