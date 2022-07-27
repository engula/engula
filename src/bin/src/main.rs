// Copyright 2022 The Engula Authors.
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

use clap::{Parser, Subcommand};
use engula_server::{Error, Result};
use tracing::info;

#[derive(Parser)]
#[clap(name = "engula", version, author, about)]
struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    fn run(self) -> Result<()> {
        self.subcmd.run()
    }
}

#[derive(Subcommand)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    fn run(self) -> Result<()> {
        match self {
            SubCommand::Start(cmd) => cmd.run(),
        }
    }
}

#[derive(Parser)]
#[clap(about = "Start engula server")]
struct StartCommand {
    #[clap(
        long,
        help = "Try to bootstrap a cluster if it not initialized, otherwise join a cluster"
    )]
    init: bool,
    #[clap(long)]
    join: Option<Vec<String>>,
    #[clap(long)]
    conf: Option<String>,
    #[clap(long)]
    addr: Option<String>,
    #[clap(long)]
    db: Option<String>,

    #[clap(long, help = "dump config as toml file and exit")]
    dump_config: Option<String>,
}

impl StartCommand {
    fn run(self) -> Result<()> {
        use engula_server::runtime::{ExecutorOwner, ShutdownNotifier, TaskPriority};

        let config = match load_config(&self) {
            Ok(c) => c,
            Err(e) => {
                return Err(Error::InvalidArgument(format!("Config: {e}")));
            }
        };

        if let Some(filename) = self.dump_config {
            let contents = toml::to_string(&config).expect("Config is serializable");
            std::fs::write(filename, contents)?;
            return Ok(());
        }

        info!("{config:#?}");

        let notifier = ShutdownNotifier::new();
        let shutdown = notifier.subscribe();
        let owner = ExecutorOwner::new(num_cpus::get());
        let executor = owner.executor();
        executor.spawn(None, TaskPriority::Low, async move {
            notifier.ctrl_c().await;
        });
        engula_server::run(config, executor, shutdown)
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(atty::is(atty::Stream::Stderr))
        .init();

    let cmd = Command::parse();
    cmd.run()
}

fn load_config(
    cmd: &StartCommand,
) -> std::result::Result<engula_server::Config, config::ConfigError> {
    use config::{Config, Environment, File};

    let mut builder = Config::builder()
        .set_default("addr", "127.0.0.1:21805")?
        .set_default("init", false)?
        .set_default("join_list", Vec::<String>::default())?;

    if let Some(conf) = cmd.conf.as_ref() {
        builder = builder.add_source(File::with_name(conf));
    }

    let c = builder
        .add_source(Environment::with_prefix("engula"))
        .set_override_option("addr", cmd.addr.clone())?
        .set_override_option("root_dir", cmd.db.clone())?
        .set_override_option("join_list", cmd.join.clone())?
        .set_override_option("init", if cmd.init { Some(true) } else { None })?
        .build()?;

    c.try_deserialize()
}
