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

pub struct Checkpoint {}

impl super::super::raft::Checkpoint for Checkpoint {
    fn apply_state(&self) -> crate::serverpb::v1::EntryId {
        todo!()
    }

    fn descriptor(&self) -> engula_api::server::v1::GroupDesc {
        todo!()
    }

    fn stable(&self, base_dir: &std::path::Path) -> crate::Result<()> {
        todo!()
    }
}
