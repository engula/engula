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

use super::{Client, Error, Result};

#[derive(Clone)]
pub struct Collection {
    name: String,
    dbname: String,
    client: Client,
}

impl Collection {
    pub(crate) fn new(name: String, dbname: String, client: Client) -> Self {
        Self {
            name,
            dbname,
            client,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn dbname(&self) -> &str {
        &self.dbname
    }

    pub async fn desc(&self) -> Result<CollectionDesc> {
        let req = DescribeCollectionRequest {
            name: self.name.clone(),
            dbname: self.dbname.clone(),
        };
        let req = universe_request::Request::DescribeCollection(req);
        let res = self.client.universe(req).await?;
        let desc = if let universe_response::Response::DescribeCollection(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing collection descriptor"))
    }
}
