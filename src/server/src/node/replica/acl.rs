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

use crate::Result;

/// A structures control access of replica.
#[allow(unused)]
#[derive(Clone)]
pub struct AccessController {}

#[allow(unused)]
pub struct AccessGuard {}

#[allow(unused)]
impl AccessController {
    pub async fn acquire(&self) -> Result<AccessGuard> {
        todo!()
    }
    /// FIXME(walter) async drop?
    pub fn release(&self) {
        todo!()
    }

    /// Allow acquiring new access token.
    pub async fn allow(&self) {
        todo!()
    }

    /// Disallow accessing and wait until all former access guard release.
    pub async fn disallow(&self) -> Result<AccessGuard> {
        todo!()
    }
}

impl Drop for AccessGuard {
    fn drop(&mut self) {
        todo!()
    }
}
