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

use crate::{universe_client::UniverseClient, Database, Error, Result};

pub struct Universe {
    client: UniverseClient,
}

impl Universe {
    pub async fn connect(url: impl Into<String>) -> Result<Universe> {
        let client = UniverseClient::connect(url).await?;
        Ok(Universe { client })
    }

    pub async fn database(&self, name: impl Into<String>) -> Result<Database> {
        let desc = self.describe_database(name).await?;
        Ok(Database::new(self.client.clone(), desc))
    }

    pub async fn create_database(&self, name: impl Into<String>) -> Result<Database> {
        let spec = DatabaseSpec { name: name.into() };
        let req = CreateDatabaseRequest { spec: Some(spec) };
        let req = databases_request_union::Request::CreateDatabase(req);
        let res = self.client.clone().databases_union(req).await?;
        if let databases_response_union::Response::CreateDatabase(res) = res {
            let desc = res.desc.ok_or(Error::InvalidResponse)?;
            Ok(Database::new(self.client.clone(), desc))
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
        self.client.clone().databases_union(req).await?;
        Ok(())
    }

    pub async fn describe_database(&self, name: impl Into<String>) -> Result<DatabaseDesc> {
        let req = DescribeDatabaseRequest {
            name: name.into(),
            ..Default::default()
        };
        let req = databases_request_union::Request::DescribeDatabase(req);
        let res = self.client.clone().databases_union(req).await?;
        if let databases_response_union::Response::DescribeDatabase(res) = res {
            res.desc.ok_or(Error::InvalidResponse)
        } else {
            Err(Error::InvalidResponse)
        }
    }
}
