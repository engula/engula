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
use std::{sync::Arc, time::Duration};

use tracing::{debug, warn};

use crate::runtime::{current, TaskPriority};

pub async fn start_purging_expired_files(engine: Arc<raft_engine::Engine>) {
    current().spawn(None, TaskPriority::IoLow, async move {
        loop {
            crate::runtime::time::sleep(Duration::from_secs(10)).await;
            let cloned_engine = engine.clone();
            match current()
                .spawn_blocking(move || cloned_engine.purge_expired_files())
                .await
            {
                Err(e) => {
                    warn!("raft engine purge expired files: {e:?}")
                }
                Ok(replicas) => {
                    if !replicas.is_empty() {
                        debug!("raft engine purge expired files, replicas {replicas:?} is too old")
                    }
                }
            }
        }
    });
}
