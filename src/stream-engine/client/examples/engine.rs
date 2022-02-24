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

use futures::StreamExt;
use stream_engine_client::{Engine, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let engine = Engine::connect(url).await?;
    let tenant = engine.create_tenant("tenant").await?;
    println!("created {:?}", tenant.desc().await?);
    let stream = tenant.create_stream("stream").await?;
    println!("created {:?}", stream.desc());
    let mut state_stream = stream.subscribe_state().await?;
    println!("current state {:?}", state_stream.next().await);
    let seq = stream.append(Box::new([0u8])).await?;
    let mut reader = stream.new_reader().await?;
    reader.seek(seq).await?;
    let event = reader.wait_next().await?;
    println!("append and read event {:?}", event);
    println!("try read {:?}", reader.try_next().await?);
    Ok(())
}
