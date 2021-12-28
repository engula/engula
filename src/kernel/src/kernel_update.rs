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

use crate::metadata::{BucketUpdate, VersionUpdate};

#[derive(Default)]
pub struct KernelUpdate {
    pub(crate) update: VersionUpdate,
}

impl KernelUpdate {
    pub fn put_meta(&mut self, name: impl Into<String>, meta: impl Into<Vec<u8>>) -> &mut Self {
        self.update.put_meta.insert(name.into(), meta.into());
        self
    }

    pub fn remove_meta(&mut self, name: impl Into<String>) -> &mut Self {
        self.update.remove_meta.push(name.into());
        self
    }

    pub fn add_stream(&mut self, name: impl Into<String>, meta: impl Into<Vec<u8>>) -> &mut Self {
        self.update.add_streams.insert(name.into(), meta.into());
        self
    }

    pub fn remove_stream(&mut self, name: impl Into<String>) -> &mut Self {
        self.update.remove_streams.push(name.into());
        self
    }

    pub fn add_bucket(&mut self, name: impl Into<String>, meta: impl Into<Vec<u8>>) -> &mut Self {
        self.update.add_buckets.insert(name.into(), meta.into());
        self
    }

    pub fn remove_bucket(&mut self, name: impl Into<String>) -> &mut Self {
        self.update.remove_buckets.push(name.into());
        self
    }

    pub fn add_object(
        &mut self,
        bucket_name: impl Into<String>,
        object_name: impl Into<String>,
        object_meta: impl Into<Vec<u8>>,
    ) -> &mut Self {
        let bucket = self
            .update
            .update_buckets
            .entry(bucket_name.into())
            .or_insert_with(|| BucketUpdate::default());
        bucket
            .add_objects
            .insert(object_name.into(), object_meta.into());
        self
    }

    pub fn remove_object(
        &mut self,
        bucket_name: impl Into<String>,
        object_name: impl Into<String>,
    ) -> &mut Self {
        let bucket = self
            .update
            .update_buckets
            .entry(bucket_name.into())
            .or_insert_with(|| BucketUpdate::default());
        bucket.remove_objects.push(object_name.into());
        self
    }
}
