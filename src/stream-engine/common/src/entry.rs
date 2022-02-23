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

/// `Entry` is the minimum unit of the journal system. A continuous entries
/// compound a stream.

#[derive(Derivative, Clone, PartialEq, Eq)]
#[derivative(Debug)]
pub enum Entry {
    /// A placeholder, used in recovery phase.
    Hole,
    Event {
        epoch: u32,
        #[derivative(Debug = "ignore")]
        event: Box<[u8]>,
    },
    /// A bridge record, which identify the end of a segment.
    Bridge { epoch: u32 },
}

impl Entry {
    // FIXME(w41ter) a better implementation.
    pub fn epoch(&self) -> u32 {
        match self {
            Entry::Event { epoch, event: _ } => *epoch,
            Entry::Bridge { epoch } => *epoch,
            _ => panic!("Entry::Hole no epoch field"),
        }
    }

    pub fn set_epoch(&mut self, target: u32) {
        match self {
            Entry::Event { epoch, event: _ } => *epoch = target,
            Entry::Bridge { epoch } => *epoch = target,
            _ => {}
        }
    }

    pub fn size(&self) -> usize {
        if let Entry::Event { event, .. } = self {
            core::mem::size_of::<Entry>() + event.len()
        } else {
            core::mem::size_of::<Entry>()
        }
    }
}
