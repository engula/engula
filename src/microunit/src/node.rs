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

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    error::{Error, Result},
    unit::{Unit, UnitBuilder, UnitDesc, UnitSpec},
};

/// A node manages a set of units.
pub struct Node {
    id: String,
    inner: Mutex<Inner>,
}

struct Inner {
    units: HashMap<String, Box<dyn Unit>>,
    unit_builders: HashMap<String, Box<dyn UnitBuilder>>,
}

impl Node {
    pub fn status(&self) -> NodeDesc {
        NodeDesc {
            id: self.id.clone(),
        }
    }

    pub async fn list_units(&self) -> Vec<UnitDesc> {
        let inner = self.inner.lock().await;
        let mut descs = Vec::new();
        for unit in inner.units.values() {
            descs.push(unit.desc().await);
        }
        descs
    }

    pub async fn create_unit(&self, spec: UnitSpec) -> Result<UnitDesc> {
        let mut inner = self.inner.lock().await;
        if let Some(builder) = inner.unit_builders.get(&spec.kind) {
            let id = Uuid::new_v4().to_string();
            let unit = builder.spawn(id.clone(), spec).await?;
            let desc = unit.desc().await;
            assert!(inner.units.insert(id, unit).is_none());
            Ok(desc)
        } else {
            Err(Error::InvalidArgument)
        }
    }

    pub async fn delete_unit(&self, _uid: &str) -> Result<()> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct NodeDesc {
    pub id: String,
}

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
        let inner = Inner {
            units: HashMap::new(),
            unit_builders: self.unit_builders,
        };
        Node {
            id: Uuid::new_v4().to_string(),
            inner: Mutex::new(inner),
        }
    }
}
