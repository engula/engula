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

use crate::{async_trait, Engine, Result};

#[async_trait]
pub trait Kernel {
    async fn engine(&self, name: impl Into<String>) -> Result<Box<dyn Engine>>;

    async fn create_engine(&self, name: impl Into<String>) -> Result<Box<dyn Engine>>;
}
