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

use crate::metadata::{BucketUpdate, KernelUpdate};

#[derive(Default)]
pub struct BucketUpdateBuilder {
    update: BucketUpdate,
}

impl BucketUpdateBuilder {
    pub fn add_object(
        &mut self,
        object_name: impl Into<String>,
        object_meta: impl Into<Vec<u8>>,
    ) -> &mut Self {
        self.update
            .add_objects
            .insert(object_name.into(), object_meta.into());
        self
    }

    pub fn remove_object(&mut self, object_name: impl Into<String>) -> &mut Self {
        self.update.remove_objects.push(object_name.into());
        self
    }

    pub fn build(&mut self) -> BucketUpdate {
        std::mem::take(&mut self.update)
    }
}

impl From<BucketUpdateBuilder> for BucketUpdate {
    fn from(mut b: BucketUpdateBuilder) -> Self {
        b.build()
    }
}

#[derive(Default)]
pub struct KernelUpdateBuilder {
    update: KernelUpdate,
}

impl KernelUpdateBuilder {
    pub fn put_meta(&mut self, name: impl Into<String>, meta: impl Into<Vec<u8>>) -> &mut Self {
        self.update.put_meta.insert(name.into(), meta.into());
        self
    }

    pub fn remove_meta(&mut self, name: impl Into<String>) -> &mut Self {
        self.update.remove_meta.push(name.into());
        self
    }

    pub fn add_stream(&mut self, name: impl Into<String>) -> &mut Self {
        self.update.add_streams.push(name.into());
        self
    }

    pub fn remove_stream(&mut self, name: impl Into<String>) -> &mut Self {
        self.update.remove_streams.push(name.into());
        self
    }

    pub fn add_bucket(&mut self, name: impl Into<String>) -> &mut Self {
        self.update.add_buckets.push(name.into());
        self
    }

    pub fn update_bucket(
        &mut self,
        name: impl Into<String>,
        update: impl Into<BucketUpdate>,
    ) -> &mut Self {
        self.update
            .update_buckets
            .insert(name.into(), update.into());
        self
    }

    pub fn remove_bucket(&mut self, name: impl Into<String>) -> &mut Self {
        self.update.remove_buckets.push(name.into());
        self
    }

    pub fn build(&mut self) -> KernelUpdate {
        std::mem::take(&mut self.update)
    }
}

impl From<KernelUpdateBuilder> for KernelUpdate {
    fn from(mut b: KernelUpdateBuilder) -> Self {
        b.build()
    }
}
