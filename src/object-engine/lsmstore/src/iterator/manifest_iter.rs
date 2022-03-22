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

use std::{collections::BTreeSet, ops::Bound::*};

use crate::{
    versions::{FileMetadata, OrdByUpperBound},
    Key,
};

pub struct ManifestIter {
    files: BTreeSet<OrdByUpperBound>,
    current: Option<OrdByUpperBound>,
    init: bool,
}

impl ManifestIter {
    pub fn new(files: BTreeSet<OrdByUpperBound>) -> Self {
        let mut last_upper_bound = None;
        for f in &files {
            if let Some(last) = last_upper_bound {
                assert!(f.lower_bound > last)
            }
            last_upper_bound = Some(f.upper_bound.to_owned());
        }
        Self {
            files,
            current: None,
            init: false,
        }
    }

    pub fn key(&self) -> Key<'_> {
        debug_assert!(self.valid());
        self.current.as_ref().unwrap().upper_bound.as_slice().into()
    }

    pub fn value(&self) -> FileMetadata {
        debug_assert!(self.valid());
        self.current.as_ref().cloned().unwrap().0
    }

    pub fn valid(&self) -> bool {
        !self.init || self.current.is_some()
    }

    pub fn seek_to_first(&mut self) {
        self.current = self.files.first().cloned();
        if !self.init {
            self.init = true;
        }
    }

    pub fn seek(&mut self, target: Key<'_>) {
        self.current = self
            .files
            .range((
                Included(OrdByUpperBound(FileMetadata {
                    upper_bound: target.to_owned(),
                    ..Default::default()
                })),
                Unbounded,
            ))
            .next()
            .cloned();
        if !self.init {
            self.init = true;
        }
    }

    pub fn next(&mut self) {
        if !self.init {
            self.seek_to_first();
            self.init = true
        }
        self.current = self
            .files
            .range((
                Excluded(OrdByUpperBound(FileMetadata {
                    upper_bound: self.current.as_ref().unwrap().upper_bound.to_owned(),
                    ..Default::default()
                })),
                Unbounded,
            ))
            .next()
            .cloned()
    }
}
