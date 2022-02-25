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

use std::collections::HashMap;

use crate::{core::Progress, Sequence};

#[inline(always)]
pub(super) fn advance_acked_sequence(
    epoch: u32,
    progresses: &HashMap<String, Progress>,
) -> Sequence {
    progresses
        .iter()
        .filter_map(|(_, p)| {
            let matched_index = p.matched_index();
            if matched_index == 0 {
                None
            } else {
                Some(Sequence::new(epoch, matched_index))
            }
        })
        .max()
        .unwrap_or_default()
}

#[inline(always)]
pub(super) fn actual_acked_index(_num_copies: usize, acked_indexes: &[u32]) -> Option<u32> {
    acked_indexes.iter().max().cloned()
}
