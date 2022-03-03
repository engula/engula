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

use clap::Parser;
use stream_engine_store::Server as StoreServer;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tracing_subscriber;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value_t = String::from("0.0.0.0:21718"))]
    endpoint: String,
}

async fn bootstrap_service(endpoint: &str) -> Result<()> {
    use tonic::transport::Server;

    let listener = TcpListener::bind(endpoint).await?;
    let store_server = StoreServer::new();
    Server::builder()
        .add_service(store_server.into_service())
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    bootstrap_service(&args.endpoint).await?;

    println!("Bye");

    Ok(())
}
