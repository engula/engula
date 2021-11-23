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

use crate::{
    proto::{UnitDesc, UnitSpec},
    unit::{Unit, UnitBuilder, UnitResult},
};

pub struct ControlUnit {
    desc: UnitDesc,
}

impl ControlUnit {
    fn new(id: String, spec: UnitSpec) -> Self {
        let desc = UnitDesc {
            id,
            kind: spec.kind,
        };
        Self { desc }
    }
}

#[async_trait]
impl Unit for ControlUnit {
    async fn desc(&self) -> UnitDesc {
        self.desc.clone()
    }

    async fn start(&self) -> UnitResult<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct ControlUnitBuilder {}

#[async_trait]
impl UnitBuilder for ControlUnitBuilder {
    fn kind(&self) -> &str {
        "ControlUnit"
    }

    async fn spawn(&self, id: String, spec: UnitSpec) -> UnitResult<Box<dyn Unit>> {
        let unit = ControlUnit::new(id, spec);
        Ok(Box::new(unit))
    }
}
