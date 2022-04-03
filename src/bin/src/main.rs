use clap::Parser;

#[derive(Parser)]
struct Command {}

impl Command {
    fn run(&self) {
        println!("Hello, Engula!");
    }
}

fn main() {
    let cmd: Command = Command::parse();
    cmd.run();
}
