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

pub struct DiskTable {
    handles: Vec<DiskHandle>,
    next: u32,
}

pub struct DiskHandle {
    pub fileno: u32,
    pub offset: u32,
    pub length: u32,
    pub next: u32,
}

impl DiskTable {
    pub fn with_capacity(capacity: usize) -> DiskTable {
        Self {
            handles: Vec::with_capacity(capacity),
            next: 0,
        }
    }

    pub fn get(&self, index: u32) -> Option<&DiskHandle> {
        self.handles.get(index as usize)
    }

    pub fn insert(&mut self, handle: DiskHandle) -> u32 {
        let index = self.next;
        if index == self.handles.len() as u32 {
            self.handles.push(handle);
            self.next = index + 1;
        } else {
            let handle = std::mem::replace(&mut self.handles[index as usize], handle);
            self.next = handle.next;
        }
        index
    }
}
