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

use std::collections::HashMap;

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    error::{Error, Result},
    unit::{Unit, UnitBuilder, UnitDesc, UnitSpec},
};

#[derive(Default)]
pub struct NodeBuilder {
    unit_builders: HashMap<String, Box<dyn UnitBuilder>>,
}

impl NodeBuilder {
    pub fn unit(mut self, builder: impl UnitBuilder + 'static) -> NodeBuilder {
        let kind = builder.kind().to_owned();
        assert!(self.unit_builders.insert(kind, Box::new(builder)).is_none());
        self
    }

    pub fn build(self) -> Node {
        let core = Core {
            units: HashMap::new(),
            unit_builders: self.unit_builders,
        };
        Node {
            core: Mutex::new(core),
        }
    }
}

/// A node manages a set of units.
pub struct Node {
    core: Mutex<Core>,
}

impl Node {
    pub async fn list_units(&self) -> Vec<UnitDesc> {
        let core = self.core.lock().await;
        let mut descs = Vec::new();
        for unit in core.units.values() {
            descs.push(unit.desc().await);
        }
        descs
    }

    pub async fn create_unit(&self, spec: UnitSpec) -> Result<UnitDesc> {
        let mut core = self.core.lock().await;
        if let Some(builder) = core.unit_builders.get(&spec.kind) {
            let id = Uuid::new_v4().to_string();
            let unit = builder.spawn(id.clone(), spec).await?;
            let desc = unit.desc().await;
            assert!(core.units.insert(id, unit).is_none());
            Ok(desc)
        } else {
            Err(Error::InvalidArgument(format!(
                "invalid unit kind '{}'",
                spec.kind
            )))
        }
    }

    pub async fn delete_unit(&self, _uid: &str) -> Result<()> {
        Ok(())
    }
}

struct Core {
    units: HashMap<String, Box<dyn Unit>>,
    unit_builders: HashMap<String, Box<dyn UnitBuilder>>,
}
