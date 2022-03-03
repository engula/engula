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

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use clap::{Parser, Subcommand};
use futures::StreamExt;
use stream_engine_client::{Engine, Error, Role, Tenant};
use tracing::info;
use tracing_subscriber;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, required = true)]
    local_id: String,

    #[clap(short, long, default_value_t = String::from("0.0.0.0:21716"))]
    master: String,

    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Streams,
    Create {
        #[clap(long, required = true)]
        stream: String,
    },
    Delete {
        #[clap(long, required = true)]
        stream: String,
    },
    Load {
        #[clap(long, required = true)]
        stream: String,
        #[clap(long, required = true)]
        path: PathBuf,
    },
    Append {
        #[clap(long, required = true)]
        stream: String,
        #[clap(long, required = true)]
        event: Vec<u8>,
    },
    Subscribe {
        #[clap(long, required = true)]
        stream: String,
        #[clap(long)]
        start: Option<u64>,
    },
}

async fn list_streams(_tenant: &Tenant) -> Result<()> {
    todo!();
}

async fn create_stream(tenant: &Tenant, stream_name: String) -> Result<()> {
    info!("try create stream {}", stream_name);
    tenant.create_stream(&stream_name).await?;
    Ok(())
}

async fn delete_stream(tenant: &mut Tenant, stream_name: String) -> Result<()> {
    info!("try delete stream {}", stream_name);
    tenant.delete_stream(&stream_name).await?;
    Ok(())
}

async fn load_events<P: AsRef<Path>>(
    tenant: &mut Tenant,
    stream_name: String,
    path: P,
) -> Result<()> {
    use tokio::{
        fs::File,
        io::{AsyncBufReadExt, BufReader},
    };

    let path = path.as_ref();
    let file = File::open(path).await?;
    let buf = BufReader::new(file);
    let mut lines = buf.lines();

    let stream = tenant.stream(&stream_name).await?;
    let mut state_stream = stream.subscribe_state().await?;

    let mut line: Option<String> = None;
    'OUTER: while let Some(state) = state_stream.next().await {
        if state.role != Role::Leader {
            continue;
        }

        loop {
            if line.is_none() {
                line = lines.next_line().await?;
                if line.is_none() {
                    // All events are consumed.
                    break 'OUTER;
                }
            }
            let content = line.as_ref().unwrap();
            match stream.append(content.as_bytes().into()).await {
                Ok(seq) => {
                    println!("{}", seq);
                    line = None;
                }
                Err(Error::NotLeader(_)) => {
                    break;
                }
                Err(err) => Err(err)?,
            };
        }
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

async fn append_event(tenant: &mut Tenant, stream_name: String, event: Vec<u8>) -> Result<()> {
    let stream = tenant.stream(&stream_name).await?;
    let mut state_stream = stream.subscribe_state().await?;
    while let Some(state) = state_stream.next().await {
        if state.role != Role::Leader {
            continue;
        }

        match stream.append(event.clone().into()).await {
            Ok(seq) => {
                println!("{}", seq);
                break;
            }
            Err(Error::NotLeader(_)) => {}
            Err(err) => Err(err)?,
        };
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

async fn subscribe_events(tenant: &Tenant, stream_name: String, start: Option<u64>) -> Result<()> {
    let stream = tenant.stream(&stream_name).await?;
    let mut reader = stream.new_reader().await?;
    reader.seek(start.unwrap_or_default().into()).await?;
    loop {
        let event = reader.wait_next().await?;
        println!("{:?}", event);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let url = "http://localhost:21716";
    let engine = Engine::new("1".to_owned(), url).await?;
    let mut tenant = match engine.create_tenant("tenant").await {
        Err(Error::AlreadyExists(_)) => {
            info!("create tenant: already exists");
            engine.tenant("tenant")
        }
        Ok(tenant) => tenant,
        Err(error) => Err(error)?,
    };

    match args.cmd {
        Command::Streams => list_streams(&tenant).await?,
        Command::Create { stream } => create_stream(&tenant, stream).await?,
        Command::Delete { stream } => delete_stream(&mut tenant, stream).await?,
        Command::Load { stream, path } => load_events(&mut tenant, stream, &path).await?,
        Command::Append { stream, event } => append_event(&mut tenant, stream, event).await?,
        Command::Subscribe { stream, start } => subscribe_events(&tenant, stream, start).await?,
    }

    println!("Bye");

    Ok(())
}
