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

use crate::{expr::call_expr, Object, Result, TypedObject};

pub struct Int64(Object);

impl TypedObject for Int64 {
    type TypedValue = i64;
}

impl From<Object> for Int64 {
    fn from(ob: Object) -> Self {
        Self(ob)
    }
}

impl Int64 {
    pub async fn add(self, value: i64) -> Result<()> {
        self.0.call(call_expr::add_assign(value)).await?;
        Ok(())
    }

    pub async fn sub(self, value: i64) -> Result<()> {
        self.0.call(call_expr::sub_assign(value)).await?;
        Ok(())
    }
}
