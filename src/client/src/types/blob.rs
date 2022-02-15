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

use crate::{Any, Object, Result, Txn};

pub struct Blob(Any);

impl Object for Blob {
    type Txn = BlobTxn;
    type Value = Vec<u8>;
}

impl From<Any> for Blob {
    fn from(ob: Any) -> Self {
        Self(ob)
    }
}

impl Blob {
    pub fn begin(self) -> BlobTxn {
        self.0.begin().into()
    }

    pub async fn len(self) -> Result<i64> {
        self.0.len().await
    }

    pub async fn append(self, value: Vec<u8>) -> Result<()> {
        self.0.append(value).await
    }
}

pub struct BlobTxn(Txn);

impl From<Txn> for BlobTxn {
    fn from(txn: Txn) -> Self {
        Self(txn)
    }
}

impl BlobTxn {
    pub fn append(&mut self, value: Vec<u8>) -> &mut Self {
        self.0.append(value);
        self
    }

    pub async fn commit(self) -> Result<()> {
        self.0.commit().await
    }
}
