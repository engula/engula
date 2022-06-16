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

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use engula_api::server::v1::*;
use tokio_stream::StreamExt;
use tonic::Streaming;
use tracing::warn;

use crate::RootClient;

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct Router {
    root_client: RootClient,
    state: Arc<Mutex<State>>,
}

#[derive(Debug, Clone, Default)]
pub struct State {
    db_id_lookup: HashMap<u64, String>,
    db_name_lookup: HashMap<String, u64>,
}

#[allow(unused)]
impl Router {
    pub async fn connect(root_addr: String) -> Result<Self, crate::Error> {
        let root_client = RootClient::connect(root_addr).await?;
        let events = root_client.watch(0).await?;
        let state = Arc::new(Mutex::new(State::default()));
        let state_clone = state.clone();
        tokio::spawn(async move {
            state_main(state_clone, events).await;
        });
        Ok(Self { root_client, state })
    }
}

async fn state_main(state: Arc<Mutex<State>>, mut events: Streaming<WatchResponse>) {
    use watch_response::{delete_event::Event as DeleteEvent, update_event::Event as UpdateEvent};

    while let Some(event) = events.next().await {
        let (updates, deletes) = match event {
            Ok(resp) => (resp.updates, resp.deletes),
            Err(status) => {
                warn!("WatchEvent error: {}", status);
                continue;
            }
        };
        for update in updates {
            let event = match update.event {
                None => continue,
                Some(event) => event,
            };
            let mut state = state.lock().unwrap();
            match event {
                UpdateEvent::Node(_) => todo!(),
                UpdateEvent::Group(_) => todo!(),
                UpdateEvent::GroupState(_) => todo!(),
                UpdateEvent::Database(db_desc) => {
                    state.db_id_lookup.insert(db_desc.id, db_desc.name.clone());
                    state.db_name_lookup.insert(db_desc.name, db_desc.id);
                }
                UpdateEvent::Collection(_) => todo!(),
            }
        }
        for delete in deletes {
            let event = match delete.event {
                None => continue,
                Some(event) => event,
            };
            let mut state = state.lock().unwrap();
            match event {
                DeleteEvent::Node(_) => todo!(),
                DeleteEvent::Group(_) => todo!(),
                DeleteEvent::Database(db_desc) => {
                    if let Some(name) = state.db_id_lookup.remove(&db_desc) {
                        state.db_name_lookup.remove(name.as_str());
                    }
                }
                DeleteEvent::Collection(_) => todo!(),
            }
        }
    }
}
