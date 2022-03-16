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

use engula_supervisor::v1::Supervisor;

use super::{apis::v1::*, Result, Universe};

#[derive(Clone)]
pub struct Cooperator {
    uv: Universe,
}

impl Cooperator {
    pub fn new(sv: Supervisor) -> Self {
        let uv = Universe::new(sv);
        Self { uv }
    }

    pub async fn batch(&self, batch_req: BatchRequest) -> Result<BatchResponse> {
        let mut batch_res = BatchResponse::default();
        for req in batch_req.databases {
            let db = self.uv.database(&req.name).await?;
            let res = db.execute(req).await?;
            batch_res.databases.push(res);
        }
        Ok(batch_res)
    }
}
