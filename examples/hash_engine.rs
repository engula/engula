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

use clap::Parser;
use engula::{
    engine::hash::{Engine, Result},
    kernel::{grpc::Kernel as KernelClient, mem::Kernel as MemKernel, Kernel},
};

#[derive(Parser)]
struct Args {
    #[clap(
        long,
        about = "The address of a Kernel server, a memory kernel instance is run if not specified"
    )]
    kernel: Option<SocketAddr>,
}

async fn run<K: Kernel>(kernel: K) -> Result<()> {
    let engine = Engine::open(kernel).await?;
    let key = vec![1];
    let value = vec![2];
    engine.put(key.clone(), value.clone()).await?;
    let got = engine.get(&key).await?;
    assert_eq!(got, Some(value));
    engine.delete(key.clone()).await?;
    let got = engine.get(&key).await?;
    assert_eq!(got, None);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg: Args = Args::parse();
    if let Some(addr) = arg.kernel {
        let kernel = KernelClient::connect(&addr.to_string()).await?;
        run(kernel).await
    } else {
        let kernel = MemKernel::open().await?;
        run(kernel).await
    }
}
