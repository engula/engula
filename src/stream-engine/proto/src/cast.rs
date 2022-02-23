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

use stream_engine_common::Entry;

use super::{ObserverState, Role};

impl From<i32> for ObserverState {
    fn from(state: i32) -> Self {
        ObserverState::from_i32(state).unwrap_or(ObserverState::Following)
    }
}

impl From<i32> for Role {
    fn from(role: i32) -> Self {
        Role::from_i32(role).unwrap_or(Role::Follower)
    }
}

impl From<Entry> for crate::Entry {
    fn from(e: Entry) -> Self {
        match e {
            Entry::Hole => crate::Entry {
                entry_type: crate::EntryType::Hole as i32,
                epoch: 0,
                event: vec![],
            },
            Entry::Event { epoch, event } => crate::Entry {
                entry_type: crate::EntryType::Event as i32,
                epoch,
                event: event.into(),
            },
            Entry::Bridge { epoch } => crate::Entry {
                entry_type: crate::EntryType::Bridge as i32,
                epoch,
                event: vec![],
            },
        }
    }
}

impl From<crate::Entry> for Entry {
    fn from(e: crate::Entry) -> Self {
        match crate::EntryType::from_i32(e.entry_type) {
            Some(crate::EntryType::Event) => Entry::Event {
                event: e.event.into(),
                epoch: e.epoch,
            },
            Some(crate::EntryType::Bridge) => Entry::Bridge { epoch: e.epoch },
            _ => Entry::Hole,
        }
    }
}
