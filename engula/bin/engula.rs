mod bench;
mod config;
mod start;

use clap::Clap;

#[derive(Clap)]
struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    Bench(bench::Command),
    Start(start::Command),
}

#[tokio::main]
async fn main() {
    let appender = tracing_appender::rolling::never("/tmp/engula/", "LOG");
    let (non_blocking, _guard) = tracing_appender::non_blocking(appender);
    tracing_subscriber::fmt().with_writer(non_blocking).init();

    let cmd: Command = Command::parse();
    match &cmd.subcmd {
        SubCommand::Bench(cmd) => cmd.run().await.unwrap(),
        SubCommand::Start(cmd) => cmd.run().await.unwrap(),
    }
}
