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

use std::{collections::HashMap, sync::Arc, time::Duration};

use stream_engine_proto::*;
use tokio::sync::Mutex;

use crate::{stream::StreamInfo, Error, Result};

#[derive(Debug, Clone)]
pub struct Config {
    /// How many tick before an observer's lease is timeout.
    ///
    /// Default: 3
    pub heartbeat_timeout_tick: u64,

    /// Observer heartbeat intervals in ms.
    ///
    /// Default: 500ms
    pub heartbeat_interval_ms: u64,
}

impl Config {
    pub fn heartbeat_timeout(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval_ms * self.heartbeat_timeout_tick)
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            heartbeat_timeout_tick: 3,
            heartbeat_interval_ms: 500,
        }
    }
}

#[derive(Clone)]
pub struct Master {
    pub config: Config,

    // FIXME(w41ter)
    // This is a temporary implementation, which needs to be
    // supported on orchestrator.
    pub stores: Vec<String>,
    inner: Arc<Mutex<MasterInner>>,
}

struct MasterInner {
    next_id: u64,
    tenants: HashMap<String, Tenant>,
}

impl Master {
    pub fn new(config: Config, stores: Vec<String>) -> Self {
        let inner = MasterInner {
            next_id: 1,
            tenants: HashMap::new(),
        };
        Self {
            config,
            stores,
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn tenant(&self, name: &str) -> Result<Tenant> {
        let inner = self.inner.lock().await;
        inner
            .tenants
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("tenant {}", name)))
    }

    pub async fn create_tenant(&self, mut desc: TenantDesc) -> Result<TenantDesc> {
        let mut inner = self.inner.lock().await;
        if inner.tenants.contains_key(&desc.name) {
            return Err(Error::AlreadyExists(format!("tenant {}", desc.name)));
        }
        desc.id = inner.next_id;
        inner.next_id += 1;
        let db = Tenant::new(desc.clone());
        inner.tenants.insert(desc.name.clone(), db);
        Ok(desc)
    }
}

#[derive(Clone)]
pub struct Tenant {
    inner: Arc<Mutex<TenantInner>>,
}

struct TenantInner {
    desc: TenantDesc,
    next_id: u64,
    streams: HashMap<u64, StreamInfo>,
}

impl Tenant {
    fn new(desc: TenantDesc) -> Self {
        let inner = TenantInner {
            desc,
            next_id: 1,
            streams: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn desc(&self) -> TenantDesc {
        self.inner.lock().await.desc.clone()
    }

    pub async fn stream_desc(&self, name: &str) -> Result<StreamDesc> {
        let inner = self.inner.lock().await;
        inner
            .streams
            .values()
            .find(|info| info.stream_name == name)
            .map(StreamInfo::stream_desc)
            .ok_or_else(|| Error::NotFound(format!("stream {}", name)))
    }

    pub async fn stream(&self, stream_id: u64) -> Result<StreamInfo> {
        let inner = self.inner.lock().await;
        inner
            .streams
            .get(&stream_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("stream id {}", stream_id)))
    }

    pub async fn create_stream(&self, mut desc: StreamDesc) -> Result<StreamDesc> {
        let mut inner = self.inner.lock().await;
        if inner
            .streams
            .values()
            .any(|info| info.stream_name == desc.name)
        {
            return Err(Error::AlreadyExists(format!("stream {}", desc.name)));
        }

        desc.id = inner.next_id;
        inner.next_id += 1;
        desc.parent_id = inner.desc.id;
        inner.streams.insert(
            desc.id,
            StreamInfo::new(desc.parent_id, desc.id, desc.name.clone()),
        );
        Ok(desc)
    }
}
