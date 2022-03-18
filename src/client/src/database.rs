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

use crate::{Client, Collection, DatabaseTxn, Error, Result};

#[derive(Clone)]
pub struct Database {
    name: String,
    client: Client,
}

impl Database {
    pub(crate) fn new(name: String, client: Client) -> Self {
        Self { name, client }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn desc(&self) -> Result<DatabaseDesc> {
        let req = DescribeDatabaseRequest {
            name: self.name.clone(),
        };
        let req = universe_request::Request::DescribeDatabase(req);
        let res = self.client.universe(req).await?;
        let desc = if let universe_response::Response::DescribeDatabase(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing database descriptor"))
    }

    pub fn begin(&self) -> DatabaseTxn {
        DatabaseTxn::new(self.name.clone(), self.client.clone())
    }

    pub fn collection(&self, name: &str) -> Collection {
        Collection::new(name.to_owned(), self.name.clone(), self.client.clone())
    }

    pub fn list_collections(&self) -> CollectionList {
        CollectionList::new(self.name.clone(), self.client.clone())
    }

    pub async fn create_collection(&self, name: &str) -> Result<Collection> {
        let req = CreateCollectionRequest {
            name: name.to_owned(),
            dbname: self.name.clone(),
            ..Default::default()
        };
        let req = universe_request::Request::CreateCollection(req);
        self.client.universe(req).await?;
        Ok(self.collection(name))
    }

    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        let req = DeleteCollectionRequest {
            name: name.to_owned(),
            dbname: self.name.clone(),
        };
        let req = universe_request::Request::DeleteCollection(req);
        self.client.universe(req).await?;
        Ok(())
    }
}

pub struct CollectionList {
    name: String,
    client: Client,
    next_page_token: Option<String>,
}

impl CollectionList {
    fn new(name: String, client: Client) -> Self {
        Self {
            name,
            client,
            next_page_token: Some(String::new()),
        }
    }
}

impl CollectionList {
    pub async fn collect(mut self) -> Result<Vec<CollectionDesc>> {
        let mut descs = Vec::new();
        loop {
            let page = self.next_page(100).await?;
            if page.is_empty() {
                break;
            }
            descs.extend(page);
        }
        Ok(descs)
    }

    pub async fn next_page(&mut self, size: usize) -> Result<Vec<CollectionDesc>> {
        if let Some(page_token) = self.next_page_token.take() {
            let req = ListCollectionsRequest {
                name: self.name.clone(),
                page_size: size as u64,
                page_token,
            };
            let req = universe_request::Request::ListCollections(req);
            let res = self.client.universe(req).await?;
            if let universe_response::Response::ListCollections(mut res) = res {
                let next_page_token = std::mem::take(&mut res.next_page_token);
                if !next_page_token.is_empty() {
                    self.next_page_token = Some(next_page_token);
                }
                Ok(std::mem::take(&mut res.descs))
            } else {
                Err(Error::internal("missing list collections response"))
            }
        } else {
            Ok(Vec::new())
        }
    }
}
