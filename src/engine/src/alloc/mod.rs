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

mod intrusive;

use std::{
    alloc::{alloc, Layout},
    ptr::NonNull,
    sync::Mutex,
};

use lazy_static::lazy_static;

use self::intrusive::{LinkedList, ListNode, ListNodeAdaptor};

const SEGMENT_SIZE: usize = 8 * 1024 * 1024;

pub const fn next_multiple_of(lhs: usize, rhs: usize) -> usize {
    match lhs % rhs {
        0 => lhs,
        r => lhs + (rhs - r),
    }
}

#[repr(C)]
struct Segment {
    node: ListNode<Segment>,

    allocated: usize,
    freed: usize,
    data: [u8; 0],
}

crate::intrusive_linked_list_adaptor!(SegmentNodeAdaptor, Segment, node);

impl Segment {
    fn try_alloc(&mut self, layout: Layout) -> Option<*mut u8> {
        let supported_align = std::mem::size_of::<usize>();
        assert!(layout.align() <= supported_align);

        let align_allocated = next_multiple_of(self.allocated, supported_align);
        if align_allocated + layout.size() > SEGMENT_SIZE {
            None
        } else {
            self.allocated = align_allocated + layout.size();
            unsafe {
                let segment_base = self as *mut _ as *mut u8;
                Some(segment_base.add(align_allocated))
            }
        }
    }

    fn free(&mut self, layout: Layout) {
        let aligned_size = next_multiple_of(layout.size(), std::mem::size_of::<usize>());
        self.freed += aligned_size;
    }

    fn is_empty(&self) -> bool {
        self.freed == SEGMENT_SIZE || self.allocated == std::mem::size_of::<Segment>()
    }

    fn try_reuse(&mut self) -> bool {
        if self.is_empty() {
            self.allocated = std::mem::size_of::<Segment>();
            self.freed = std::mem::size_of::<Segment>();
            true
        } else {
            false
        }
    }
}

impl Segment {
    fn new() -> Box<Segment> {
        let layout = Layout::from_size_align(SEGMENT_SIZE, SEGMENT_SIZE).unwrap();
        unsafe {
            let segment_header_size = std::mem::size_of::<Segment>();
            let default_segment = Segment {
                node: ListNode::new(),
                allocated: segment_header_size,
                freed: segment_header_size,
                data: [0; 0],
            };
            let mut ptr = NonNull::new_unchecked(alloc(layout)).cast::<Segment>();
            let uninit_segment = ptr.as_uninit_mut();
            uninit_segment.write(default_segment);
            Box::from_raw(ptr.as_ptr())
        }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        if !self.is_empty() {
            panic!("There are some memory leaks");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_new() {
        let segment = Segment::new();
        println!("address {:X}", segment.as_ref() as *const Segment as usize);
    }
}

type SegmentList = LinkedList<Segment, SegmentNodeAdaptor>;

pub struct Lsa {
    segments: SegmentList,
    freed_segments: SegmentList,
}

impl Lsa {
    pub fn new() -> Self {
        Lsa {
            segments: SegmentList::new(),
            freed_segments: SegmentList::new(),
        }
    }

    pub fn alloc(&mut self, layout: Layout) -> *mut u8 {
        for _ in 0..2 {
            if let Some(last_segment) = self.segments.back_mut() {
                if let Some(addr) = last_segment.try_alloc(layout) {
                    return addr;
                }
            }

            self.alloc_segment();
        }

        panic!("couldn't alloc enough memory with {:?}", layout);
    }

    fn alloc_segment(&mut self) {
        if let Some(segment) = self.freed_segments.pop_front() {
            self.segments.push_back(segment);
        } else {
            self.segments.push_back(Segment::new());
        }
    }

    unsafe fn might_reuse_segment(&mut self, mut segment: NonNull<Segment>) {
        if segment.as_mut().try_reuse() {
            let boxed_segment = self.segments.unlink_node(segment);
            self.freed_segments.push_back(boxed_segment);
        }
    }
}

// Because we don't obtain segment references without lock.
unsafe impl Send for Lsa {}

lazy_static! {
    static ref GLOBAL_LSA: Mutex<Lsa> = Mutex::new(Lsa::new());
}

pub fn lsa_alloc(layout: Layout) -> *mut u8 {
    let mut lsa = GLOBAL_LSA.lock().unwrap();
    lsa.alloc(layout)
}

pub fn lsa_dealloc(addr: *mut u8, layout: Layout) {
    let mut lsa = GLOBAL_LSA.lock().unwrap();
    unsafe {
        let segment_addr = addr as usize ^ (SEGMENT_SIZE - 1);
        if let Some(mut segment_ptr) = NonNull::new(segment_addr as *mut Segment) {
            let segment = segment_ptr.as_mut();
            segment.free(layout);
            lsa.might_reuse_segment(segment_ptr);
        }
    }
}

#[allow(dead_code)]
fn assert_segment_size_is_power_of_two() {
    let _: [u8; SEGMENT_SIZE] = [0; (SEGMENT_SIZE - 1).next_power_of_two()];
}
