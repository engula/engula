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

use std::{fs, time::Duration};

use anyhow::{anyhow, Result};
use clap::Parser;
use engula_server::Config;
use toml::Value;

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
    #[clap(long, default_value = "127.0.0.1:21716")]
    addr: String,

    #[clap(long, default_value = "mio", arg_enum)]
    driver_mode: DriverMode,

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
                let values = config.parse::<toml::Value>()?;
                Some(values)
            }
        };

        let config = merge_configs(self, values)?;

        engula_server::run(config)?;

        Ok(())
    }
}

const DEFAULT_CONNECTION_TIMEOUT: Option<Duration> = None;

/// Merge configs from args and files, and prefer those from args to those from file.
fn merge_configs(args: StartCommand, values: Option<Value>) -> Result<Config> {
    let addr = args.addr;

    let driver_mode = args.driver_mode.into();

    let mut connection_timeout = DEFAULT_CONNECTION_TIMEOUT;
    if let Some(values) = values {
        if let Some(timeout) = values.get("timeout") {
            match timeout {
                Value::Integer(timeout) if *timeout >= 0 => {
                    connection_timeout = Some(Duration::from_secs(*timeout as u64));
                }
                timeout => {
                    return Err(anyhow!(
                        "timeout should be non-negative integer, but {:?}",
                        timeout
                    ))
                }
            }
        }
    }
    if let Some(timeout) = args.timeout {
        connection_timeout = Some(Duration::from_secs(timeout as u64));
    }

    Ok(Config {
        addr,
        driver_mode,
        connection_timeout,
    })
}
