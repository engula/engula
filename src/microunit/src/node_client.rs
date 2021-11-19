// Copyright 2021 The Engula Authors.
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

use reqwest::Client;

use super::{error::Result, node::NodeDesc};

pub struct NodeClient {
    url: String,
    client: Client,
}

impl NodeClient {
    pub fn new(url: &str) -> NodeClient {
        NodeClient {
            url: format!("{}/v1", url),
            client: Client::new(),
        }
    }

    pub async fn status(&self) -> Result<NodeDesc> {
        let url = format!("{}/status", self.url);
        let desc = self.client.get(url).send().await?.json().await?;
        Ok(desc)
    }
}
