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

use std::sync::Arc;

use engula_apis::*;
use tonic::transport::Endpoint;

use crate::{txn_client::TxnClient, universe_client::UniverseClient, Database, Error, Result};

#[derive(Clone)]
pub struct Universe {
    inner: Arc<UniverseInner>,
}

impl Universe {
    pub async fn connect(url: impl Into<String>) -> Result<Universe> {
        // Assumes that users run a standalone server for now so that we can reuse the
        // same url here.
        let chan = Endpoint::new(url.into())
            .map_err(Error::unknown)?
            .connect()
            .await
            .map_err(Error::unknown)?;
        let inner = UniverseInner {
            txn_client: TxnClient::new(chan.clone()),
            universe_client: UniverseClient::new(chan),
        };
        Ok(Universe {
            inner: Arc::new(inner),
        })
    }

    pub fn database(&self, name: impl Into<String>) -> Database {
        self.inner.new_database(name)
    }

    pub async fn create_database(&self, name: impl Into<String>) -> Result<Database> {
        let name = name.into();
        let spec = DatabaseSpec { name: name.clone() };
        let req = CreateDatabaseRequest { spec: Some(spec) };
        let req = database_request_union::Request::CreateDatabase(req);
        self.inner.database_union_call(req).await?;
        Ok(self.database(name))
    }

    pub async fn delete_database(&self, name: impl Into<String>) -> Result<()> {
        let req = DeleteDatabaseRequest { name: name.into() };
        let req = database_request_union::Request::DeleteDatabase(req);
        self.inner.database_union_call(req).await?;
        Ok(())
    }
}

struct UniverseInner {
    txn_client: TxnClient,
    universe_client: UniverseClient,
}

impl UniverseInner {
    fn new_database(&self, name: impl Into<String>) -> Database {
        Database::new(
            name.into(),
            self.txn_client.clone(),
            self.universe_client.clone(),
        )
    }

    async fn database_union_call(
        &self,
        req: database_request_union::Request,
    ) -> Result<database_response_union::Response> {
        self.universe_client.clone().database_union(req).await
    }
}
