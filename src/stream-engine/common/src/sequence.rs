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

/// An increasing number to order events.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct Sequence {
    pub epoch: u32,
    pub index: u32,
}

impl Sequence {
    pub fn new(epoch: u32, index: u32) -> Self {
        Sequence { epoch, index }
    }

    pub fn is_continuously(lhs: &Sequence, rhs: &Sequence) -> bool {
        lhs.epoch == rhs.epoch && lhs.index + 1 == rhs.index
    }
}

impl From<u64> for Sequence {
    fn from(v: u64) -> Self {
        Sequence {
            epoch: (v >> 32) as u32,
            index: (v as u32),
        }
    }
}

impl From<Sequence> for u64 {
    fn from(seq: Sequence) -> Self {
        (seq.epoch as u64) << 32 | (seq.index as u64)
    }
}

impl std::fmt::Display for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", <Sequence as Into<u64>>::into(*self))
    }
}
