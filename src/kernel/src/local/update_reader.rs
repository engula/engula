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

use tokio::sync::broadcast::{error::TryRecvError, Receiver};

use crate::{async_trait, Error, Result, UpdateEvent};

pub struct UpdateReader {
    rx: Receiver<UpdateEvent>,
}

impl UpdateReader {
    pub fn new(rx: Receiver<UpdateEvent>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl crate::UpdateReader for UpdateReader {
    async fn try_next(&mut self) -> Result<Option<UpdateEvent>> {
        match self.rx.try_recv() {
            Ok(v) => Ok(Some(v)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(err) => Err(Error::unknown(err)),
        }
    }

    async fn wait_next(&mut self) -> Result<UpdateEvent> {
        self.rx.recv().await.map_err(Error::unknown)
    }
}
