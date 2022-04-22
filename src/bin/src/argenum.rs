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

///! Adapters for [`clap::ArgEnum`].

#[derive(clap::ArgEnum, Clone)]
pub enum DriverMode {
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
