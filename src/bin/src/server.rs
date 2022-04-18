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

use anyhow::Result;
use clap::Parser;
use engula_server::Config;

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

#[derive(clap::ArgEnum, Clone)]
enum DriverMode {
    Mio,
    #[cfg(target_os = "linux")]
    Uio,
}

impl From<DriverMode> for engula_server::DriverMode {
    fn from(mode: DriverMode) -> Self {
        match mode {
            DriverMode::Mio => engula_server::DriverMode::Mio,
            #[cfg(target_os = "linux")]
            DriverMode::Uio => engula_server::DriverMode::Uio,
        }
    }
}

#[derive(Parser)]
struct StartCommand {
    #[clap(long, default_value = "127.0.0.1:21716")]
    addr: String,
    #[clap(long, default_value = "mio", arg_enum)]
    driver_mode: DriverMode,
    #[clap(long, default_value = "0", help = "the limit of memory usage, in KB")]
    max_memory: usize,
}

impl StartCommand {
    fn run(self) -> Result<()> {
        let mut config = Config {
            addr: self.addr,
            driver_mode: self.driver_mode.into(),
            max_memory_kb: self.max_memory,
        };
        #[cfg(target_os = "linux")]
        if config.max_memory_kb == 0 {
            use sys::*;
            let mem_info = MemInfo::new()?;
            config.max_memory_kb = (mem_info.total as f64 * 0.8) as usize;
        }

        engula_server::run(config)?;
        Ok(())
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
