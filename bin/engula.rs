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

use clap::{crate_version, Args, Parser, Subcommand};
use engula_journal::{
    file::Journal as FileJournal, grpc::Server as JournalServer, mem::Journal as MemJournal,
    Error as JournalError,
};
use engula_kernel::{
    grpc::{FileKernel, MemKernel, Server as KernelServer},
    Error as KernelError,
};
use engula_storage::{
    file::Storage as FileStorage, grpc::Server as StorageServer, mem::Storage as MemStorage,
    Error as StorageError,
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

macro_rules! run_until_asked_to_quit {
    ($addr:expr, $server:expr) => {{
        let cloned_addr = $addr.clone();
        let (tx, rx) = oneshot::channel();
        let mut server_handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service($server.into_service())
                .serve_with_shutdown(cloned_addr, async {
                    rx.await.unwrap_or_default();
                })
                .await
        });

        let mut server_is_quit = false;
        tokio::select! {
            resp = &mut server_handle => {
                server_is_quit = true;
                resp.unwrap_or(Ok(()))?;
            }
            _ = signal::ctrl_c() => {
                let _ = tx.send(());
            }
        }
        if !server_is_quit {
            server_handle.await.unwrap_or(Ok(()))?;
        }
    }};
}

#[derive(Subcommand)]
enum MemOrFile {
    #[clap(name = "--mem", about = "An instance stores everything in memory")]
    Mem,
    #[clap(
        name = "--file",
        about = "An instance stores everything in local files"
    )]
    File {
        #[clap(parse(from_os_str), about = "Path to store data")]
        path: PathBuf,
    },
}

#[derive(Args)]
struct EnvArgs {
    #[clap(about = "Socket address to listen")]
    addr: SocketAddr,
}

#[derive(Subcommand)]
#[clap(about = "Commands to operate Storage")]
enum StorageCommand {
    #[clap(about = "Run a storage server")]
    Run {
        #[clap(flatten)]
        env: EnvArgs,

        #[clap(subcommand)]
        cmd: MemOrFile,
    },
}

impl StorageCommand {
    async fn run(&self) -> Result<()> {
        match self {
            StorageCommand::Run { env, cmd } => match cmd {
                MemOrFile::File { path } => {
                    let storage = FileStorage::new(&path).await?;
                    let server = StorageServer::new(storage);
                    run_until_asked_to_quit!(&env.addr, server);
                }
                MemOrFile::Mem => {
                    let server = StorageServer::new(MemStorage::default());
                    run_until_asked_to_quit!(&env.addr, server);
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
        #[clap(flatten)]
        env: EnvArgs,

        #[clap(subcommand)]
        cmd: MemOrFile,

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
                env,
                cmd,
                segment_size,
            } => match cmd {
                MemOrFile::File { path } => {
                    let journal = FileJournal::open(path, *segment_size).await?;
                    let server = JournalServer::new(journal);
                    run_until_asked_to_quit!(&env.addr, server);
                }
                MemOrFile::Mem => {
                    let server = JournalServer::new(MemJournal::default());
                    run_until_asked_to_quit!(&env.addr, server);
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
        #[clap(flatten)]
        env: EnvArgs,

        #[clap(subcommand)]
        cmd: MemOrFile,

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
                env,
                cmd,
                journal,
                storage,
            } => match cmd {
                MemOrFile::Mem => {
                    let kernel =
                        MemKernel::open(&journal.to_string(), &storage.to_string()).await?;
                    let server = KernelServer::new(kernel);
                    run_until_asked_to_quit!(&env.addr, server);
                }
                MemOrFile::File { path } => {
                    let kernel =
                        FileKernel::open(&journal.to_string(), &storage.to_string(), &path).await?;
                    let server = KernelServer::new(kernel);
                    run_until_asked_to_quit!(&env.addr, server);
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
    author = "The Engula Authors",
    about = "Engula is a serverless storage engine that empowers engineers to build reliable and cost-effective databases."
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
