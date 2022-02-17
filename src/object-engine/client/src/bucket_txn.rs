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

use engula_apis::Expr;

use crate::Result;

pub struct BucketTxn {}

impl BucketTxn {
    pub fn add_expr(&mut self, _expr: Expr) {
        todo!();
    }

    pub fn add_metadata(&mut self, _key: Vec<u8>, _value: Vec<u8>) {
        todo!();
    }

    pub fn remove_metadata(&mut self, _key: Vec<u8>) {
        todo!();
    }

    pub async fn commit(self) -> Result<()> {
        todo!();
    }
}
