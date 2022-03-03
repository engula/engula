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
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
};

use tokio::{
    runtime::Handle as RuntimeHandle,
    sync::{mpsc, Mutex},
};

use crate::{
    group::{ActionChannel, EventChannel, Worker, WorkerOption},
    master::{Master, Stream as StreamClient},
    stream::Stream,
    tenant::Tenant,
    EpochState, Result,
};

#[derive(Clone)]
pub struct Engine {
    pub(crate) master: Master,

    inner: Arc<Mutex<EngineInner>>,
}

impl Engine {
    pub async fn new(id: String, url: impl Into<String>) -> Result<Self> {
        Ok(Engine {
            master: Master::new(url).await?,
            inner: Arc::new(Mutex::new(EngineInner::new(id).await)),
        })
    }

    #[inline(always)]
    pub fn tenant(&self, name: &str) -> Tenant {
        Tenant::new(name, self.to_owned())
    }

    #[inline(always)]
    pub async fn create_tenant(&self, name: &str) -> Result<Tenant> {
        let tenant_client = self.master.create_tenant(name).await?;
        Ok(Tenant::new_with_client(self.to_owned(), tenant_client))
    }

    #[inline(always)]
    pub async fn delete_tenant(&self, name: &str) -> Result<()> {
        self.master.delete_tenant(name).await
    }
}

impl Engine {
    pub(crate) async fn create_stream(&self, stream_client: StreamClient) -> Result<Stream> {
        let stream_id = stream_client.stream_id();
        let mut inner = self.inner.lock().await;
        let channel = if let Some(channel) = inner.streams.get(&stream_id) {
            channel.clone()
        } else {
            let channel = EventChannel::new(stream_id);
            inner.streams.insert(stream_id, channel.clone());
            inner
                .active_channel
                .add(channel.clone(), stream_client.clone())
                .await?
                .expect("insert stream should success");
            channel
        };

        Ok(Stream::new(self.clone(), stream_client, channel))
    }

    pub(crate) async fn observe_stream(
        &self,
        stream_id: u64,
    ) -> Result<mpsc::UnboundedReceiver<EpochState>> {
        let (sender, receiver) = mpsc::unbounded_channel();

        let inner = self.inner.lock().await;
        inner.active_channel.observe(stream_id, sender).await??;
        Ok(receiver)
    }

    pub async fn close(&self) {
        let mut inner = self.inner.lock().await;
        if let Some((join_handle, flag)) = inner.worker_handle.take() {
            flag.store(true, Ordering::Release);
            join_handle.join().unwrap_or_default();
        }
    }
}

struct EngineInner {
    worker_handle: Option<(JoinHandle<()>, Arc<AtomicBool>)>,
    streams: HashMap<u64, EventChannel>,
    active_channel: ActionChannel,
}

impl EngineInner {
    async fn new(id: String) -> Self {
        let opt = WorkerOption {
            observer_id: id,
            heartbeat_interval_ms: 500,
            runtime_handle: RuntimeHandle::current(),
        };
        let worker = Worker::new(opt);
        let active_channel = worker.action_channel();
        let exit_flag = Arc::new(AtomicBool::new(false));
        let cloned_exit_flag = exit_flag.clone();
        let join_handle = thread::spawn(move || {
            Worker::run(worker, cloned_exit_flag);
        });
        EngineInner {
            worker_handle: Some((join_handle, exit_flag)),
            streams: HashMap::new(),
            active_channel,
        }
    }
}

impl Drop for EngineInner {
    fn drop(&mut self) {
        if let Some((_, flag)) = self.worker_handle.take() {
            flag.store(true, Ordering::Release);
        }
    }
}
