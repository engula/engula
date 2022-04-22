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

use std::time::Duration;

#[derive(Debug, Copy, Clone)]
pub enum DriverMode {
    Mio,
    #[cfg(target_os = "linux")]
    Uio,
}

pub struct Config {
    pub addr: String,
    pub driver_mode: DriverMode,
    pub connection_timeout: Option<Duration>,
}

pub struct ConfigBuilder {
    pub addr: String,
    pub driver_mode: DriverMode,
    pub connection_timeout: Option<Duration>,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:21716".to_string(),
            driver_mode: DriverMode::Mio,
            connection_timeout: None,
        }
    }
}

impl ConfigBuilder {
    pub fn build(self) -> Config {
        Config {
            addr: self.addr,
            driver_mode: self.driver_mode,
            connection_timeout: self.connection_timeout,
        }
    }
}
