use std::sync::Arc;

use clap::Clap;
use engula::*;
use tonic::transport::Server;

#[derive(Clap)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn run(&self) -> Result<()> {
        match &self.subcmd {
            SubCommand::Journal(cmd) => cmd.run().await?,
            SubCommand::Storage(cmd) => cmd.run().await?,
            SubCommand::Compaction(cmd) => cmd.run().await?,
        }
        Ok(())
    }
}

#[derive(Clap)]
enum SubCommand {
    Journal(JournalCommand),
    Storage(StorageCommand),
    Compaction(CompactionCommand),
}

#[derive(Clap, Debug)]
struct JournalCommand {
    #[clap(long)]
    addr: String,
    #[clap(long)]
    path: String,
    #[clap(long)]
    sync: bool,
}

impl JournalCommand {
    async fn run(&self) -> Result<()> {
        println!("{:?}", self);
        let addr = self.addr.parse().unwrap();
        let options = JournalOptions {
            sync: self.sync,
            chunk_size: 1024,
        };
        let service = JournalService::new(&self.path, options)?;
        Server::builder()
            .add_service(JournalServer::new(service))
            .serve(addr)
            .await?;
        Ok(())
    }
}

#[derive(Clap, Debug)]
struct StorageCommand {
    #[clap(long)]
    addr: String,
    #[clap(long)]
    path: String,
}

impl StorageCommand {
    async fn run(&self) -> Result<()> {
        println!("{:?}", self);
        let addr = self.addr.parse().unwrap();
        let fs = LocalFs::new(&self.path)?;
        let service = FsService::new(Box::new(fs));
        Server::builder()
            .add_service(FsServer::new(service))
            .serve(addr)
            .await?;
        Ok(())
    }
}

#[derive(Clap, Debug)]
struct CompactionCommand {
    #[clap(long)]
    addr: String,
    #[clap(long)]
    storage_url: String,
}

impl CompactionCommand {
    async fn run(&self) -> Result<()> {
        println!("{:?}", self);
        let addr = self.addr.parse().unwrap();
        let options = SstOptions::default();
        let storage = SstStorage::new(&self.storage_url, options).await?;
        let runtime = LocalCompaction::new(Arc::new(storage));
        let service = CompactionService::new(Box::new(runtime));
        Server::builder()
            .add_service(CompactionServer::new(service))
            .serve(addr)
            .await?;
        Ok(())
    }
}
