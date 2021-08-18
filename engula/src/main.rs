use clap::{crate_version, Clap};

use engula_supreme_unit::SupremeUnit;
use microunit::cmd::{NodeCommand, UnitCommand};
use microunit::NodeBuilder;

#[derive(Clap)]
#[clap(version = crate_version!())]
struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub fn run(&self) {
        let unit = SupremeUnit::new();
        let node = NodeBuilder::new().add_unit(unit).build();
        match &self.subcmd {
            SubCommand::Node(cmd) => cmd.run(node),
            SubCommand::Unit(cmd) => cmd.run(),
        }
    }
}

#[derive(Clap)]
enum SubCommand {
    Node(NodeCommand),
    Unit(UnitCommand),
}

fn main() {
    let cmd: Command = Command::parse();
    cmd.run();
}
