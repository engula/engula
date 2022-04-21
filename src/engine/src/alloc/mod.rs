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
    ptr::{addr_of_mut, NonNull},
    sync::Mutex,
};

use lazy_static::lazy_static;

const SEGMENT_SIZE: usize = 8 * 1024 * 1024;

pub const fn next_multiple_of(lhs: usize, rhs: usize) -> usize {
    match lhs % rhs {
        0 => lhs,
        r => lhs + (rhs - r),
    }
}

#[repr(C)]
struct Segment {
    next: Option<NonNull<Segment>>,
    prev: Option<NonNull<Segment>>,

    allocated: usize,
    data: [u8; 0],
}

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
}

impl Segment {
    fn new() -> Box<Segment> {
        let layout = Layout::from_size_align(SEGMENT_SIZE, SEGMENT_SIZE).unwrap();
        unsafe {
            let mut ptr = NonNull::new_unchecked(alloc(layout)).cast::<Segment>();
            addr_of_mut!((*ptr.as_mut()).next).write(None);
            addr_of_mut!((*ptr.as_mut()).prev).write(None);
            (*ptr.as_mut()).allocated = std::mem::size_of::<Segment>();
            Box::from_raw(ptr.as_ptr())
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

pub struct Lsa {
    head: Option<NonNull<Segment>>,
    tail: Option<NonNull<Segment>>,
    num_segments: usize,
}

impl Lsa {
    pub fn new() -> Self {
        Lsa {
            head: None,
            tail: None,
            num_segments: 0,
        }
    }

    pub fn alloc(&mut self, layout: Layout) -> *mut u8 {
        if self.num_segments == 0 {
            let segment = Segment::new();
            self.head = Some(NonNull::from(Box::leak(segment)));
            self.tail = self.head;
            self.num_segments += 1;
        }

        for _ in 0..2 {
            let last_segment = unsafe { self.tail.as_mut().unwrap().as_mut() };
            if let Some(addr) = last_segment.try_alloc(layout) {
                return addr;
            }

            self.push_segment(Segment::new());
        }

        panic!("couldn't alloc enough memory with {:?}", layout);
    }

    fn push_segment(&mut self, mut segment: Box<Segment>) {
        segment.prev = self.tail;
        let tail_node = Some(NonNull::from(Box::leak(segment)));

        let last_segment = unsafe { self.tail.as_mut().unwrap().as_mut() };
        last_segment.next = tail_node;
        self.tail = tail_node;
        self.num_segments += 1;
    }
}

impl Drop for Lsa {
    fn drop(&mut self) {
        if let Some(node) = self.head.take() {
            let mut node = unsafe { Box::from_raw(node.as_ptr()) };
            while let Some(next) = node.next.take() {
                node.prev = None;
                node = unsafe { Box::from_raw(next.as_ptr()) };
            }
            node.prev = None;
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

pub fn lsa_dealloc(_addr: *mut u8, _layout: Layout) {
    // ignore, maybe we could update statistics in here.
}
