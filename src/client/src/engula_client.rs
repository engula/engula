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

use std::sync::{Arc, Mutex};

use engula_api::v1::*;

use crate::{AdminRequestBuilder, AdminResponseExtractor, RootClient, Router};

#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<Mutex<ClientInner>>,
}

#[derive(Debug, Clone)]
struct ClientInner {
    root_client: RootClient,
    router: Router,
}

impl Client {
    pub async fn connect(addr: String) -> Result<Self, crate::Error> {
        let root_client = RootClient::connect(addr.clone()).await?;
        let router = Router::connect(addr).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(ClientInner {
                root_client,
                router,
            })),
        })
    }

    pub async fn create_database(&self, name: String) -> Result<DatabaseDesc, crate::Error> {
        let inner = self.inner.lock().unwrap();
        let root_client = inner.root_client.clone();
        let resp = root_client
            .admin(AdminRequestBuilder::create_database(name.clone()))
            .await?;
        match AdminResponseExtractor::create_database(resp) {
            None => Err(crate::Error::DatabaseNotFound(name)),
            Some(desc) => Ok(desc),
        }
    }

    pub async fn open_database(&self, name: String) -> Result<Database, crate::Error> {
        let inner = self.inner.lock().unwrap();
        let root_client = inner.root_client.clone();
        let resp = root_client
            .admin(AdminRequestBuilder::get_database(name.clone()))
            .await?;
        match AdminResponseExtractor::get_database(resp) {
            None => Err(crate::Error::DatabaseNotFound(name)),
            Some(desc) => Ok(Database {
                desc,
                client: self.clone(),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Database {
    desc: DatabaseDesc,
    client: Client,
}
