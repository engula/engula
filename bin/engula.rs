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

use std::{net::SocketAddr, path::PathBuf};

use clap::{crate_description, crate_version, Parser, Subcommand};
use engula_journal::{
    file::Journal as FileJournal, grpc::Server as JournalServer, mem::Journal as MemJournal,
};
use engula_kernel::grpc::{FileKernel, MemKernel, Server as KernelServer};
use engula_storage::{
    file::Storage as FileStorage, grpc::Server as StorageServer, mem::Storage as MemStorage,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

macro_rules! bootstrap_service {
    ($addr:expr, $server:expr) => {{
        let listener = TcpListener::bind($addr).await?;
        tonic::transport::Server::builder()
            .add_service($server.into_service())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await?;
    }};
}

#[derive(Subcommand)]
enum RunMode {
    #[clap(name = "--mem", about = "Stores data in memory")]
    Mem,
    #[clap(name = "--file", about = "Stores data in local files")]
    File {
        #[clap(parse(from_os_str), about = "Path to store data")]
        path: PathBuf,
    },
}

#[derive(Subcommand)]
#[clap(about = "Commands to operate Storage")]
enum StorageCommand {
    #[clap(about = "Run a storage server")]
    Run {
        #[clap(about = "Socket address to listen")]
        addr: String,

        #[clap(subcommand)]
        cmd: RunMode,
    },
}

impl StorageCommand {
    async fn run(&self) -> Result<()> {
        match self {
            StorageCommand::Run { addr, cmd } => match cmd {
                RunMode::File { path } => {
                    let storage = FileStorage::new(&path).await?;
                    let server = StorageServer::new(storage);
                    bootstrap_service!(addr, server);
                }
                RunMode::Mem => {
                    let server = StorageServer::new(MemStorage::default());
                    bootstrap_service!(addr, server);
                }
            },
        }
        Ok(())
    }
}

#[derive(Subcommand)]
#[clap(about = "Commands to operate Journal")]
enum JournalCommand {
    #[clap(about = "Run a journal server")]
    Run {
        #[clap(about = "Socket address to listen")]
        addr: String,

        #[clap(subcommand)]
        cmd: RunMode,

        #[clap(
            long,
            default_value = "67108864",
            about = "The size of segments in bytes, only taking effects for a file instance"
        )]
        segment_size: usize,
    },
}

impl JournalCommand {
    async fn run(&self) -> Result<()> {
        match self {
            JournalCommand::Run {
                addr,
                cmd,
                segment_size,
            } => match cmd {
                RunMode::File { path } => {
                    let journal = FileJournal::open(path, *segment_size).await?;
                    let server = JournalServer::new(journal);
                    bootstrap_service!(addr, server);
                }
                RunMode::Mem => {
                    let server = JournalServer::new(MemJournal::default());
                    bootstrap_service!(addr, server);
                }
            },
        }
        Ok(())
    }
}

#[derive(Subcommand)]
#[clap(about = "Commands to operate Kernel")]
enum KernelCommand {
    #[clap(about = "Run a kernel server")]
    Run {
        #[clap(about = "Socket address to listen")]
        addr: String,

        #[clap(subcommand)]
        mode: RunMode,

        #[clap(long, about = "The address of journal server")]
        journal: SocketAddr,

        #[clap(long, about = "The address of storage server")]
        storage: SocketAddr,
    },
}

impl KernelCommand {
    async fn run(&self) -> Result<()> {
        match self {
            KernelCommand::Run {
                addr,
                mode: cmd,
                journal,
                storage,
            } => match cmd {
                RunMode::Mem => {
                    let kernel =
                        MemKernel::open(&journal.to_string(), &storage.to_string()).await?;
                    let server =
                        KernelServer::new(&journal.to_string(), &storage.to_string(), kernel);
                    bootstrap_service!(addr, server);
                }
                RunMode::File { path } => {
                    let kernel =
                        FileKernel::open(&journal.to_string(), &storage.to_string(), &path).await?;
                    let server =
                        KernelServer::new(&journal.to_string(), &storage.to_string(), kernel);
                    bootstrap_service!(addr, server);
                }
            },
        }
        Ok(())
    }
}

#[derive(Parser)]
enum SubCommand {
    #[clap(subcommand)]
    Storage(StorageCommand),
    #[clap(subcommand)]
    Journal(JournalCommand),
    #[clap(subcommand)]
    Kernel(KernelCommand),
}

#[derive(Parser)]
#[clap(
    version = crate_version!(),
    about = crate_description!(),
)]
struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    async fn run(&self) -> Result<()> {
        match &self.subcmd {
            SubCommand::Storage(cmd) => cmd.run().await?,
            SubCommand::Journal(cmd) => cmd.run().await?,
            SubCommand::Kernel(cmd) => cmd.run().await?,
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd: Command = Command::parse();
    cmd.run().await
}
