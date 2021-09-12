use clap::Clap;
use engula::{JournalServer, JournalService, LocalJournal, Result};
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
        }
        Ok(())
    }
}

#[derive(Clap)]
enum SubCommand {
    Journal(JournalCommand),
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
        let journal = Box::new(LocalJournal::new(&self.path, self.sync)?);
        let service = JournalService::new(journal);
        Server::builder()
            .add_service(JournalServer::new(service))
            .serve(addr)
            .await?;
        Ok(())
    }
}
