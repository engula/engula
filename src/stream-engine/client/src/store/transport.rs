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

use std::{collections::HashMap, sync::Arc};

use stream_engine_proto::{SealRequest, WriteRequest};
use tokio::sync::Mutex;
use tonic::transport::Endpoint;

use super::client::StoreClient;
use crate::Result;

pub struct Transport {
    inner: Arc<Mutex<TransportInner>>,
}

struct TransportInner {
    clients: HashMap<String, StoreClient>,
    stream_set: HashMap<u64, ()>,
}

#[allow(dead_code)]
impl Transport {
    pub async fn new() -> Self {
        Transport {
            inner: Arc::new(Mutex::new(TransportInner {
                clients: HashMap::new(),
                stream_set: HashMap::new(),
            })),
        }
    }

    pub async fn register(&self, stream_id: u64) {
        let mut inner = self.inner.lock().await;
        inner.stream_set.insert(stream_id, ());
    }

    pub async fn unregister(&self, stream_id: u64) {
        let mut inner = self.inner.lock().await;
        inner.stream_set.remove(&stream_id);
    }

    pub async fn write(
        &self,
        target: String,
        stream_id: u64,
        writer_epoch: u32,
        write: WriteRequest,
    ) -> Result<()> {
        let client = self.get_client(target).await?;
        client.write(stream_id, writer_epoch, write).await?;
        Ok(())
    }

    pub async fn seal(
        &self,
        target: String,
        stream_id: u64,
        writer_epoch: u32,
        segment_epoch: u32,
    ) -> Result<()> {
        let client = self.get_client(target).await?;
        client
            .seal(stream_id, writer_epoch, SealRequest { segment_epoch })
            .await?;
        Ok(())
    }

    async fn get_client(&self, target: String) -> Result<StoreClient> {
        let inner = self.inner.lock().await;
        if let Some(client) = inner.clients.get(&target) {
            Ok(client.clone())
        } else {
            drop(inner);

            // FIXME(w41ter) too many concurrent connections.
            let channel = Endpoint::new(target.clone())?.connect().await?;
            let client = StoreClient::new(channel);
            let mut inner = self.inner.lock().await;
            inner.clients.insert(target, client.clone());
            Ok(client)
        }
    }
}
