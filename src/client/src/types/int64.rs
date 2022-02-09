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

use crate::{Collection, Object, Result};

pub struct Int64Object(Object);

impl From<Object> for Int64Object {
    fn from(ob: Object) -> Self {
        Self(ob)
    }
}

impl Int64Object {
    pub async fn get(self) -> Result<i64> {
        Ok(0)
    }

    pub async fn set(self, _value: i64) -> Result<()> {
        Ok(())
    }

    pub async fn delete(self) -> Result<()> {
        Ok(())
    }

    pub async fn add(self, _value: i64) -> Result<()> {
        Ok(())
    }
}

pub struct Int64Collection(Collection);

impl From<Collection> for Int64Collection {
    fn from(co: Collection) -> Self {
        Self(co)
    }
}

impl Int64Collection {
    pub fn object(&self, object_id: impl Into<Vec<u8>>) -> Int64Object {
        self.0.object(object_id).into()
    }
}
