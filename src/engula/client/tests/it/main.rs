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

async fn create_universe() -> Result<Universe> {
    let interval = option_env!("RETRY_INTERVAL").unwrap_or("1").parse()?;
    let retry = option_env!("RETRY_TIMES").unwrap_or("3").parse()?;
    let port = option_env!("ENGULA_UNIVERSE_PORT").unwrap_or("21716");

    for _ in 0..retry {
        let uv = Universe::connect(format!("http://0.0.0.0:{}", port)).await;
        match uv {
            Ok(uv) => return Ok(uv),
            _ => tokio::time::sleep(Duration::from_secs(interval)).await,
        }
    }

    Err(Error::msg(format!("Exceeds retry times: {}", retry)))
}
