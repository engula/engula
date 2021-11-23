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

use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    control_unit::ControlUnitBuilder,
    error::{Error, Result},
    proto::*,
    unit::{Unit, UnitBuilder},
};

/// A node manages a set of units.
pub struct Node {
    inner: Mutex<Inner>,
}

impl Default for Node {
    fn default() -> Self {
        let mut inner = Inner::default();
        let control_unit = ControlUnitBuilder::default();
        inner
            .unit_builders
            .insert(control_unit.kind().to_owned(), Box::new(control_unit));
        Self {
            inner: Mutex::new(inner),
        }
    }
}

#[derive(Default)]
struct Inner {
    units: HashMap<String, Arc<dyn Unit>>,
    unit_builders: HashMap<String, Box<dyn UnitBuilder>>,
}

impl Node {
    pub async fn desc(&self) -> NodeDesc {
        NodeDesc::default()
    }

    pub async fn list_units(&self) -> Result<UnitDescList> {
        let inner = self.inner.lock().await;
        let mut descs = Vec::new();
        for unit in inner.units.values() {
            descs.push(unit.desc().await);
        }
        Ok(descs)
    }

    pub async fn create_unit(&self, spec: UnitSpec) -> Result<UnitDesc> {
        let mut inner = self.inner.lock().await;
        if let Some(builder) = inner.unit_builders.get(&spec.kind) {
            let id = Uuid::new_v4().to_string();
            let unit = builder.spawn(id.clone(), spec).await?;
            let unit: Arc<dyn Unit> = Arc::from(unit);
            {
                let unit = unit.clone();
                tokio::spawn(async move { unit.start().await });
            }
            assert!(inner.units.insert(id, unit.clone()).is_none());
            Ok(unit.desc().await)
        } else {
            Err(Error::InvalidArgument(format!(
                "invalid unit kind '{}'",
                spec.kind
            )))
        }
    }
}
