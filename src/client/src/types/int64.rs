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

use crate::{Any, Object, Result};

pub struct Int64(Any);

impl Object for Int64 {
    type Value = i64;
}

impl From<Any> for Int64 {
    fn from(ob: Any) -> Self {
        Self(ob)
    }
}

impl Int64 {
    pub async fn add(self, value: i64) -> Result<()> {
        self.0.add(value).await
    }

    pub async fn sub(self, value: i64) -> Result<()> {
        self.0.sub(value).await
    }
}
