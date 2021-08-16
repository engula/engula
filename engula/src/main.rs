use clap::Clap;

use microunit::NodeCommand;

#[derive(Clap)]
struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub fn run(&self) {
        match &self.subcmd {
            SubCommand::Node(cmd) => cmd.run(),
        }
    }
}

#[derive(Clap)]
enum SubCommand {
    Node(NodeCommand),
}

fn main() {
    let cmd: Command = Command::parse();
    cmd.run();
}
