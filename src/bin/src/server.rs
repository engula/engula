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

        let mut config = merge_configs(self, values);
        #[cfg(target_os = "linux")]
        {
            use sys::*;
            let mem_info = MemInfo::new()?;
            if config.max_memory == 0 {
                config.max_memory = (mem_info.total as f64 * 0.9) as u64;
            }
            if config.compact_memory == 0 {
                config.compact_memory = (mem_info.total as f64 * 0.8) as u64;
            }
        }

        engula_server::run(config)?;

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

#[cfg(target_os = "linux")]
mod sys {
    use std::{io, str::FromStr};

    #[derive(Default, Debug)]
    pub struct MemInfo {
        pub total: usize,
        pub free: usize,
        pub available: usize,
        pub buffers: usize,
        pub cached: usize,
        pub swap_total: usize,
        pub swap_free: usize,
    }

    impl MemInfo {
        pub fn new() -> io::Result<MemInfo> {
            let mut mem_info = MemInfo::default();
            let content = std::fs::read_to_string("/proc/meminfo")?;
            for line in content.split('\n') {
                let mut iter = line.split(':');
                let field = match iter.next() {
                    Some("MemTotal") => &mut mem_info.total,
                    Some("MemFree") => &mut mem_info.free,
                    Some("MemAvailable") => &mut mem_info.available,
                    Some("Buffers") => &mut mem_info.buffers,
                    Some("Cached") => &mut mem_info.cached,
                    Some("SwapTotal") => &mut mem_info.swap_total,
                    Some("SwapFree") => &mut mem_info.swap_free,
                    _ => continue,
                };
                *field = iter
                    .next()
                    .and_then(|s| s.trim_start().split(' ').next())
                    .and_then(|s| usize::from_str(s).ok())
                    .unwrap_or_default();
            }
            Ok(mem_info)
        }
    }
}
