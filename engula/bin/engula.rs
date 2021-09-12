mod bench;

use clap::Clap;

#[derive(Clap)]
struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    Bench(bench::Command),
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let cmd: Command = Command::parse();
    let future = match &cmd.subcmd {
        SubCommand::Bench(cmd) => cmd.run(),
    };
    future.await.unwrap();
}
