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

use std::sync::Arc;

use async_trait::async_trait;
use axum::Server;
use tokio::sync::Mutex;

use crate::{
    control::Control,
    control_api,
    proto::{UnitDesc, UnitSpec},
    unit::{Unit, UnitBuilder, UnitResult},
};

pub struct ControlUnit {
    desc: Mutex<UnitDesc>,
}

impl ControlUnit {
    async fn new(id: String, spec: UnitSpec) -> UnitResult<Self> {
        let desc = UnitDesc {
            id,
            kind: spec.kind,
            addr: None,
        };
        Ok(Self {
            desc: Mutex::new(desc),
        })
    }
}

#[async_trait]
impl Unit for ControlUnit {
    async fn desc(&self) -> UnitDesc {
        self.desc.lock().await.clone()
    }

    async fn start(&self) -> UnitResult<()> {
        let addr = ([0, 0, 0, 0], 0).into();
        let ctrl = Arc::new(Control::default());
        let router = control_api::route(ctrl);
        let server = Server::bind(&addr).serve(router.into_make_service());
        let local_addr = server.local_addr();
        {
            let mut desc = self.desc.lock().await;
            desc.addr = Some(local_addr.to_string());
        }
        if let Err(err) = server.await {
            return Err(Box::new(err));
        }
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
        let unit = ControlUnit::new(id, spec).await?;
        Ok(Box::new(unit))
    }
}
