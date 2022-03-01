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

mod group;
mod simple;

use std::collections::HashMap;

pub(crate) use group::GroupReader;

use crate::{core::Progress, Sequence};

#[allow(unused)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Policy {
    /// A simple strategy that allows ack entries as long as one copy holds the
    /// dataset.
    ///
    /// This strategy is mainly used for testing.
    Simple,
}

#[allow(dead_code)]
impl Policy {
    /// Calculate the `acked_sequence` based on the matched indexes.
    pub(super) fn advance_acked_sequence(
        self,
        epoch: u32,
        progresses: &HashMap<String, Progress>,
    ) -> Sequence {
        match self {
            Policy::Simple => simple::advance_acked_sequence(epoch, progresses),
        }
    }

    /// Return the actual acked index, `None` if the indexes aren't enough.
    #[inline(always)]
    pub(super) fn actual_acked_index(
        self,
        num_copies: usize,
        acked_indexes: &[u32],
    ) -> Option<u32> {
        match self {
            Policy::Simple => simple::actual_acked_index(num_copies, acked_indexes),
        }
    }

    pub(super) fn is_enough_targets_acked(
        self,
        index: u32,
        progresses: &HashMap<String, Progress>,
    ) -> bool {
        match self {
            Policy::Simple => simple::is_enough_targets_acked(index, progresses),
        }
    }

    pub(super) fn new_group_reader(
        self,
        epoch: u32,
        next_index: u32,
        copies: Vec<String>,
    ) -> GroupReader {
        GroupReader::new(self.into(), epoch, next_index, copies)
    }
}
