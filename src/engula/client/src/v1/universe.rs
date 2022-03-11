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

use engula_apis::v1::*;
use tonic::transport::Channel;

use super::{Client, Database, Error, Result};

#[derive(Clone)]
pub struct Universe {
    client: Client,
}

impl Universe {
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        let channel = Channel::from_shared(url.into())
            .map_err(|e| Error::InvalidArgument(e.to_string()))?
            .connect()
            .await?;
        let client = Client::new(channel);
        Ok(Self { client })
    }

    pub fn database(&self, name: &str) -> Database {
        Database::new(name.to_owned(), self.client.clone())
    }

    pub fn list_databases(&self) -> DatabaseList {
        DatabaseList::new(self.client.clone())
    }

    pub async fn create_database(&self, name: &str) -> Result<Database> {
        let req = CreateDatabaseRequest {
            name: name.to_owned(),
            ..Default::default()
        };
        let req = universe_request::Request::CreateDatabase(req);
        self.client.universe(req).await?;
        Ok(self.database(name))
    }

    pub async fn delete_database(&self, name: &str) -> Result<()> {
        let req = DeleteDatabaseRequest {
            name: name.to_owned(),
        };
        let req = universe_request::Request::DeleteDatabase(req);
        self.client.universe(req).await?;
        Ok(())
    }
}

pub struct DatabaseList {
    client: Client,
    next_page_token: String,
}

impl DatabaseList {
    fn new(client: Client) -> Self {
        Self {
            client,
            next_page_token: String::new(),
        }
    }
}

impl DatabaseList {
    pub async fn next_page(&mut self, size: usize) -> Result<Vec<DatabaseDesc>> {
        let req = ListDatabasesRequest {
            page_size: size as u64,
            page_token: self.next_page_token.clone(),
        };
        let req = universe_request::Request::ListDatabases(req);
        let res = self.client.universe(req).await?;
        if let universe_response::Response::ListDatabases(mut res) = res {
            self.next_page_token = std::mem::take(&mut res.next_page_token);
            Ok(std::mem::take(&mut res.descs))
        } else {
            Err(Error::internal("missing list databases response"))
        }
    }
}
