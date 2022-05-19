// Copyright 2022 Engula Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::proto::{MemberId, NodeDescriptor};

#[derive(Debug, Clone)]
pub struct Master {
    inner: Arc<Mutex<MasterInner>>,
}

#[derive(Debug, Clone)]
struct MasterInner {
    next_node_id: u64,
    nodes: HashMap<u64, NodeDescriptor>,
}

impl Default for Master {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MasterInner::default())),
        }
    }
}

impl Default for MasterInner {
    fn default() -> Self {
        Self {
            next_node_id: 255,
            nodes: HashMap::new(),
        }
    }
}

impl Master {
    pub fn lookup_member(&self, id: &MemberId) -> Option<NodeDescriptor> {
        let inner = self.inner.lock().unwrap();
        inner.lookup_member(id)
    }

    pub fn join_member(&self, desc: NodeDescriptor) -> MemberId {
        let mut inner = self.inner.lock().unwrap();
        inner.join_member(desc)
    }

    pub fn leave_member(&self, id: &MemberId) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.leave_member(id)
    }
}

impl MasterInner {
    fn lookup_member(&self, id: &MemberId) -> Option<NodeDescriptor> {
        self.nodes.get(&id.id).cloned()
    }

    fn join_member(&mut self, desc: NodeDescriptor) -> MemberId {
        let id = self.next_node_id();
        self.nodes.insert(id.id, desc);
        id
    }

    fn leave_member(&mut self, id: &MemberId) -> bool {
        self.nodes.remove(&id.id).is_some()
    }

    fn next_node_id(&mut self) -> MemberId {
        self.next_node_id += 1;
        MemberId {
            id: self.next_node_id,
        }
    }
}
