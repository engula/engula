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

use super::{async_trait, error::Result};

/// An interface to manipulate an object.
#[async_trait]
pub trait Object {
    /// Returns the size of the object.
    async fn size(&self) -> Result<usize>;

    /// Reads a range from the object at a specific offset.
    async fn read_at(&self, buf: &mut [u8], offset: usize) -> Result<usize>;
}
