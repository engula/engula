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

use std::{
    alloc::{alloc, Layout},
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use super::{array::Array, BoxRecord, Record, RecordLayout};
use crate::objects::Tag;

#[repr(C)]
pub struct ListNode {
    pub prev: Option<NonNull<Record<ListNode>>>,
    pub next: Option<NonNull<Record<ListNode>>>,
    data: Array,
}

impl ListNode {
    pub(super) fn new(len: u32) -> Self {
        ListNode {
            prev: None,
            next: None,
            data: Array::new(len),
        }
    }
}

impl Deref for ListNode {
    type Target = Array;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for ListNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl Drop for ListNode {
    fn drop(&mut self) {
        assert!(self.prev.is_none());
        assert!(self.next.is_none());
    }
}

impl RecordLayout for ListNode {
    fn record_type() -> Tag {
        Tag::RECORD_LIST_NODE
    }

    fn layout(val: &Record<Self>) -> Layout {
        type Target = Record<ListNode>;

        let align = std::mem::align_of::<Target>();
        let fixed_size = std::mem::size_of::<Target>();
        Layout::from_size_align(fixed_size + val.data.capacity(), align).unwrap()
    }
}

impl BoxRecord<ListNode> {
    pub fn with_capacity(size: usize) -> BoxRecord<ListNode> {
        type Target = Record<ListNode>;

        let align = std::mem::align_of::<Target>();
        let fixed_size = std::mem::size_of::<Target>();
        let layout = Layout::from_size_align(fixed_size + size, align).unwrap();
        unsafe {
            let mut ptr = NonNull::new_unchecked(alloc(layout) as *mut Target);
            let uninit_record = ptr.as_uninit_mut();
            uninit_record.write(Record::new(ListNode::new(size as u32)));
            BoxRecord::from_raw(ptr)
        }
    }
}
