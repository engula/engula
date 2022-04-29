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

use std::io::Result;

pub struct ActiveFile {}

impl ActiveFile {
    pub fn seal(self) -> SealedFile {
        SealedFile {}
    }

    pub async fn read(&self, offset: u32, length: u32) -> Result<Vec<u8>> {
        todo!()
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<(u32, u32)> {
        todo!()
    }
}

pub struct SealedFile {}

impl SealedFile {
    pub async fn read(&self, offset: u32, length: u32) -> Result<Vec<u8>> {
        todo!()
    }
}
