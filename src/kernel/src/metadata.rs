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

pub mod v1 {
    tonic::include_proto!("engula.metadata.v1");
}

pub use v1::{Version, VersionUpdate};

/// An increasing number to order versions.
pub type Sequence = u64;

impl Version {
    pub fn update(&mut self, update: &VersionUpdate) {
        self.sequence = update.sequence;
        for meta in &update.add_meta {
            self.meta.insert(meta.0.clone(), meta.1.clone());
        }
        for name in &update.remove_meta {
            self.meta.remove(name);
        }
        for desc in &update.add_objects {
            self.objects.push(desc.clone());
        }
        for name in &update.remove_objects {
            if let Some(index) = self.objects.iter().position(|x| x == name) {
                self.objects.remove(index);
            }
        }
    }
}
