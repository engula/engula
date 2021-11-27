// Copyright 2021 The Engula Authors.
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

use async_trait::async_trait;

use crate::proto::{UnitDesc, UnitSpec};

pub type UnitError = Box<dyn std::error::Error + Send + Sync>;
pub type UnitResult<T> = Result<T, UnitError>;

/// A unit handle.
#[async_trait]
pub trait Unit: Send + Sync {
    async fn desc(&self) -> UnitDesc;

    async fn start(&self) -> UnitResult<()>;
}

/// A unit builder that spawns a specific kind of units.
#[async_trait]
pub trait UnitBuilder: Send + Sync {
    fn kind(&self) -> &str;

    async fn spawn(&self, id: String, spec: UnitSpec) -> UnitResult<Box<dyn Unit>>;
}
