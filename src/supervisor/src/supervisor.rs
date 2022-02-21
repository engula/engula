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
use tonic::Request;

use crate::{apis::supervisor_server::Supervisor as _, Error, Result, Server};

pub struct Supervisor {
    server: Server,
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Supervisor {
    pub fn new() -> Self {
        Self {
            server: Server::new(),
        }
    }

    pub async fn database(&self, req: DatabaseRequest) -> Result<DatabaseResponse> {
        let req = Request::new(req);
        let res = self.server.database(req).await?;
        Ok(res.into_inner())
    }

    async fn database_union(
        &self,
        req: database_request_union::Request,
    ) -> Result<database_response_union::Response> {
        let req = DatabaseRequest {
            requests: vec![DatabaseRequestUnion { request: Some(req) }],
        };
        let mut res = self.database(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or_else(|| Error::internal("missing database response"))
    }

    pub async fn describe_database(&self, name: String) -> Result<DatabaseDesc> {
        let req = DescribeDatabaseRequest { name };
        let req = database_request_union::Request::DescribeDatabase(req);
        let res = self.database_union(req).await?;
        let desc = if let database_response_union::Response::DescribeDatabase(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing database description"))
    }

    pub async fn collection(&self, req: CollectionRequest) -> Result<CollectionResponse> {
        let req = Request::new(req);
        let res = self.server.collection(req).await?;
        Ok(res.into_inner())
    }

    async fn collection_union(
        &self,
        dbname: String,
        req: collection_request_union::Request,
    ) -> Result<collection_response_union::Response> {
        let req = CollectionRequest {
            dbname,
            requests: vec![CollectionRequestUnion { request: Some(req) }],
        };
        let mut res = self.collection(req).await?;
        res.responses
            .pop()
            .and_then(|x| x.response)
            .ok_or_else(|| Error::internal("missing collection response"))
    }

    pub async fn describe_collection(
        &self,
        dbname: String,
        coname: String,
    ) -> Result<CollectionDesc> {
        let req = DescribeCollectionRequest { name: coname };
        let req = collection_request_union::Request::DescribeCollection(req);
        let res = self.collection_union(dbname, req).await?;
        let desc = if let collection_response_union::Response::DescribeCollection(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or_else(|| Error::internal("missing collection description"))
    }
}
