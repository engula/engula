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

use std::net::SocketAddr;

use reqwest::{Client, Url};

use crate::{error::Result, proto::*};

pub struct ControlClient {
    url: Url,
    client: Client,
}

impl ControlClient {
    pub fn from_addr(addr: &SocketAddr) -> Result<Self> {
        let url = Url::parse(&format!("http://{}/v1/", addr))?;
        let client = Client::new();
        Ok(Self { url, client })
    }

    pub async fn list_nodes(&self) -> Result<NodeDescList> {
        let url = self.url.join("nodes")?;
        let list = self.client.get(url).send().await?.json().await?;
        Ok(list)
    }
}
