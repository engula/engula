use clap::Clap;
use engula::*;
use tonic::transport::Server;

use super::config::Config;

#[derive(Clap)]
pub struct Command {
    #[clap(short, long)]
    config: String,
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn run(&self) -> Result<()> {
        let config = Config::from_file(&self.config).unwrap();
        println!("{:#?}", config);
        match &self.subcmd {
            SubCommand::Journal(cmd) => cmd.run(&config).await?,
            SubCommand::Storage(cmd) => cmd.run(&config).await?,
            SubCommand::Manifest(cmd) => cmd.run(&config).await?,
            SubCommand::Compaction(cmd) => cmd.run(&config).await?,
        }
        Ok(())
    }
}

#[derive(Clap)]
enum SubCommand {
    Journal(JournalCommand),
    Storage(StorageCommand),
    Manifest(ManifestCommand),
    Compaction(CompactionCommand),
}

#[derive(Clap, Debug)]
struct JournalCommand {
    #[clap(long)]
    addr: String,
}

impl JournalCommand {
    async fn run(&self, config: &Config) -> Result<()> {
        let addr = self.addr.parse().unwrap();
        let options = config.journal.as_options();
        println!(
            "start journal addr {} path {}",
            self.addr, config.journal.path,
        );
        let service = JournalService::new(&config.journal.path, options)?;
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
}

impl StorageCommand {
    async fn run(&self, config: &Config) -> Result<()> {
        let addr = self.addr.parse().unwrap();
        println!(
            "start storage addr {} path {}",
            self.addr, config.storage.path,
        );
        let service = FsService::new(&config.storage.path)?;
        Server::builder()
            .add_service(FsServer::new(service))
            .serve(addr)
            .await?;
        Ok(())
    }
}

#[derive(Clap, Debug)]
struct ManifestCommand {
    #[clap(long)]
    addr: String,
}

impl ManifestCommand {
    async fn run(&self, config: &Config) -> Result<()> {
        println!("start manifest addr {}", self.addr);
        let addr = self.addr.parse().unwrap();
        let options = config.compute.as_manifest_options();
        let storage = config.new_storage(true).await?;
        let compaction = config.new_compaction().await?;
        let manifest = LocalManifest::new(options, storage, compaction);
        let service = ManifestService::new(Box::new(manifest));
        Server::builder()
            .add_service(ManifestServer::new(service))
            .serve(addr)
            .await?;
        Ok(())
    }
}

#[derive(Clap, Debug)]
struct CompactionCommand {
    #[clap(long)]
    addr: String,
}

impl CompactionCommand {
    async fn run(&self, config: &Config) -> Result<()> {
        println!("start compaction addr {}", self.addr);
        let addr = self.addr.parse().unwrap();
        let storage = config.new_storage(false).await?;
        let runtime = LocalCompaction::new(storage);
        let service = CompactionService::new(Box::new(runtime));
        Server::builder()
            .add_service(CompactionServer::new(service))
            .serve(addr)
            .await?;
        Ok(())
    }
}
