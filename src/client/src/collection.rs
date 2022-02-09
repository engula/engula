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

use crate::{universe_client::UniverseClient, Error, Result};

pub struct Collection {
    client: UniverseClient,
    desc: CollectionDesc,
}

impl Collection {
    pub fn new(client: UniverseClient, desc: CollectionDesc) -> Self {
        Self { client, desc }
    }

    pub async fn desc(&self) -> Result<CollectionDesc> {
        let req = DescribeCollectionRequest {
            id: self.desc.id,
            ..Default::default()
        };
        let req = collections_request_union::Request::DescribeCollection(req);
        let res = self
            .client
            .clone()
            .collections_union(self.desc.parent_id, req)
            .await?;
        if let collections_response_union::Response::DescribeCollection(res) = res {
            res.desc.ok_or(Error::InvalidResponse)
        } else {
            Err(Error::InvalidResponse)
        }
    }
}
