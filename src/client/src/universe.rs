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

use engula_apis::*;
use tonic::transport::Endpoint;

use crate::{txn_client::TxnClient, universe_client::UniverseClient, Database, Error, Result};

pub struct Universe {
    txn_client: TxnClient,
    universe_client: UniverseClient,
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
        let txn_client = TxnClient::new(chan.clone());
        let universe_client = UniverseClient::new(chan);
        Ok(Universe {
            txn_client,
            universe_client,
        })
    }

    pub async fn database(&self, name: impl Into<String>) -> Result<Database> {
        let desc = self.describe_database(name).await?;
        Ok(Database::new(
            desc,
            self.txn_client.clone(),
            self.universe_client.clone(),
        ))
    }

    pub async fn create_database(&self, name: impl Into<String>) -> Result<Database> {
        let spec = DatabaseSpec { name: name.into() };
        let req = CreateDatabaseRequest { spec: Some(spec) };
        let req = databases_request_union::Request::CreateDatabase(req);
        let res = self.universe_client.clone().databases_union(req).await?;
        if let databases_response_union::Response::CreateDatabase(res) = res {
            let desc = res.desc.ok_or(Error::InvalidResponse)?;
            Ok(Database::new(
                desc,
                self.txn_client.clone(),
                self.universe_client.clone(),
            ))
        } else {
            Err(Error::InvalidResponse)
        }
    }

    pub async fn delete_database(&self, name: impl Into<String>) -> Result<()> {
        let req = DeleteDatabaseRequest {
            name: name.into(),
            ..Default::default()
        };
        let req = databases_request_union::Request::DeleteDatabase(req);
        self.universe_client.clone().databases_union(req).await?;
        Ok(())
    }

    pub async fn describe_database(&self, name: impl Into<String>) -> Result<DatabaseDesc> {
        let req = DescribeDatabaseRequest {
            name: name.into(),
            ..Default::default()
        };
        let req = databases_request_union::Request::DescribeDatabase(req);
        let res = self.universe_client.clone().databases_union(req).await?;
        if let databases_response_union::Response::DescribeDatabase(res) = res {
            res.desc.ok_or(Error::InvalidResponse)
        } else {
            Err(Error::InvalidResponse)
        }
    }
}
