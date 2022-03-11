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

tonic::include_proto!("objectengine.manifest.v1");

#[derive(Default)]
pub struct VersionEditBuilder {
    ve: VersionEdit,
}

#[allow(dead_code)]
impl VersionEditBuilder {
    pub fn add_buckets(&mut self, buckets: Vec<version_edit::Bucket>) -> &mut Self {
        self.ve.add_buckets.extend(buckets);
        self
    }

    pub fn remove_buckets(&mut self, buckets: Vec<String>) -> &mut Self {
        self.ve.remove_buckets.extend(buckets);
        self
    }

    pub fn add_ranges(&mut self, ranges: Vec<version_edit::Range>) -> &mut Self {
        self.ve.add_ranges.extend(ranges);
        self
    }

    pub fn remove_ranges(&mut self, range: Vec<version_edit::RangeId>) -> &mut Self {
        self.ve.remove_ranges.extend(range);
        self
    }

    pub fn add_files(&mut self, files: Vec<version_edit::File>) -> &mut Self {
        self.ve.add_files.extend(files);
        self
    }

    pub fn remove_files(&mut self, files: Vec<version_edit::FileId>) -> &mut Self {
        self.ve.remove_files.extend(files);
        self
    }

    pub fn add_metas(&mut self, metas: Vec<version_edit::MetaEntry>) -> &mut Self {
        self.ve.add_metas.extend(metas);
        self
    }

    pub fn remove_metas(&mut self, metas: Vec<String>) -> &mut Self {
        self.ve.remove_metas.extend(metas);
        self
    }

    pub fn set_next_file_num(&mut self, next_file_num: u64) -> &mut Self {
        self.ve.next_file_num = next_file_num;
        self
    }

    pub fn build(&self) -> VersionEdit {
        self.ve.to_owned()
    }
}
