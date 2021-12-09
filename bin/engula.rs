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

use clap::{crate_version, Parser};
use engula_journal::{
    grpc::Server as JournalServer, mem::Journal as MemJournal, Error as JournalError, Journal,
};
use engula_kernel::{
    grpc::Server as KernelServer,
    mem::{Kernel as MemKernel, Manifest},
    Error as KernelError,
};
use engula_storage::{
    grpc::Server as StorageServer, mem::Storage as MemStorage, Error as StorageError, Storage,
};
use thiserror::Error;
use tokio::{signal, sync::oneshot};

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    #[error("corrupted: {0}")]
    Corrupted(String),
    #[error(transparent)]
    Kernel(#[from] KernelError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl Error {
    pub fn corrupted<E: ToString>(err: E) -> Self {
        Self::Corrupted(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Parser)]
#[clap(version = crate_version!())]
struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    async fn run(&self) -> Result<()> {
        match &self.subcmd {
            SubCommand::Hello => println!("Hello, Engula!"),
            SubCommand::Run(args) => run(args).await?,
        }
        Ok(())
    }
}

#[derive(Parser)]
enum SubCommand {
    Hello,
    Run(RunArgs),
}

#[derive(Parser, Debug)]
struct RunArgs {
    #[clap(long, default_value = "127.0.0.1")]
    ip: std::net::IpAddr,

    #[clap(long)]
    port: u16,
}

impl RunArgs {
    fn address(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port)
    }
}

async fn run(args: &RunArgs) -> Result<()> {
    let journal = MemJournal::default();
    let storage = MemStorage::default();
    let manifest = Manifest::default();

    let stream = journal.create_stream("DEFAULT").await?;
    let bucket = storage.create_bucket("DEFAULT").await?;
    let kernel = MemKernel::init(stream, bucket, manifest).await?;
    let kernel_server = KernelServer::new(kernel);
    let journal_server = JournalServer::new(journal);
    let storage_server = StorageServer::new(storage);

    let addr = args.address();
    let (tx, rx) = oneshot::channel();
    let server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(kernel_server.into_service())
            .add_service(journal_server.into_service())
            .add_service(storage_server.into_service())
            .serve_with_shutdown(addr, async {
                rx.await.unwrap_or_default();
            })
            .await
    });

    signal::ctrl_c().await?;
    let _ = tx.send(());

    server_handle.await.unwrap_or(Ok(()))?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd: Command = Command::parse();
    cmd.run().await
}
