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

use std::fs;

use anyhow::Result;
use clap::Parser;
use engula_server::{Config, ConfigBuilder};

use crate::argenum::DriverMode;

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub fn run(self) -> Result<()> {
        match self.subcmd {
            SubCommand::Start(cmd) => cmd.run(),
        }
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

#[derive(Parser)]
struct StartCommand {
    /// Address of the Engula server.
    #[clap(long)]
    addr: Option<String>,

    /// Driver mode of the Engula server.
    #[clap(long, arg_enum)]
    driver_mode: Option<DriverMode>,

    /// Close the connection after a client is idle for N seconds (0 to disable).
    #[clap(long)]
    timeout: Option<u64>,

    /// Path to toml config file.
    #[clap(short, long)]
    config: Option<String>,
}

impl StartCommand {
    fn run(self) -> Result<()> {
        let values = match &self.config {
            None => None,
            Some(config) => {
                let config = fs::read_to_string(config)?;
                let values: ConfigBuilder = toml::from_str(&config)?;
                Some(values)
            }
        };

        engula_server::run(merge_configs(self, values))?;

        Ok(())
    }
}

/// Merge configs from args and files, and prefer those from args to those from file.
fn merge_configs(args: StartCommand, values: Option<ConfigBuilder>) -> Config {
    let mut config_builder = ConfigBuilder::default();
    apply_from_values(&mut config_builder, values);
    apply_from_args(&mut config_builder, args);
    config_builder.build()
}

fn apply_from_values(config_builder: &mut ConfigBuilder, values: Option<ConfigBuilder>) {
    let values = match values {
        None => return,
        Some(values) => values,
    };

    if let Some(timeout) = values.timeout {
        config_builder.timeout = Some(timeout);
    }
}

fn apply_from_args(config_builder: &mut ConfigBuilder, args: StartCommand) {
    if let Some(addr) = args.addr {
        config_builder.addr = Some(addr);
    }
    if let Some(driver_mode) = args.driver_mode {
        config_builder.driver_mode = Some(driver_mode.into());
    }
    if let Some(timeout) = args.timeout {
        config_builder.timeout = Some(timeout as u64);
    }
}
