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

mod api;

use std::time::Duration;

use anyhow::{Error, Result};
use engula_client::Universe;
use hyper::{Client, StatusCode};

async fn wait_for_liveness() -> Result<()> {
    let interval = option_env!("RETRY_INTERVAL").unwrap_or("1").parse()?;
    let retry = option_env!("RETRY_TIMES").unwrap_or("3").parse()?;
    let port = option_env!("ENGULA_LIVENESS_PORT").unwrap_or("21715");

    let client = Client::new();

    for _ in 0..retry {
        let uri = format!("http://0.0.0.0:{}/liveness", port).parse()?;
        let resp = client.get(uri).await;
        let status = resp.map(|r| r.status()).ok();
        match status {
            Some(StatusCode::OK) => return Ok(()),
            _ => tokio::time::sleep(Duration::from_secs(interval)).await,
        }
    }

    Err(Error::msg(format!("Exceeds retry times: {}", retry)))
}

async fn create_universe() -> Result<Universe> {
    let port = option_env!("ENGULA_UNIVERSE_PORT").unwrap_or("21716");
    let uv = Universe::connect(format!("http://0.0.0.0:{}", port)).await?;
    Ok(uv)
}
