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
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use lazy_static::lazy_static;

use self::intrusive::{LinkedList, ListNode, ListNodeAdaptor};

const SEGMENT_SIZE: usize = 8 * 1024 * 1024;
const MAX_OBJECT_SIZE: usize = 2 * 1024 * 1024;
const RECORD_ALIGN: usize = std::mem::size_of::<usize>();

pub const fn next_multiple_of(lhs: usize, rhs: usize) -> usize {
    match lhs % rhs {
        0 => lhs,
        r => lhs + (rhs - r),
    }
}

#[repr(C, align(8))]
struct Segment {
    node: ListNode<Segment>,

    allocated: usize,
    freed: usize,
    compacting: bool,
    active: bool,
    data: [u8; 0],
}

crate::intrusive_linked_list_adaptor!(SegmentNodeAdaptor, Segment, node);

impl Segment {
    fn try_alloc(&mut self, layout: Layout) -> Option<*mut u8> {
        assert!(layout.align() <= RECORD_ALIGN);

        let align_allocated = next_multiple_of(self.allocated, RECORD_ALIGN);
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
        let aligned_size = next_multiple_of(layout.size(), RECORD_ALIGN);
        self.freed += aligned_size;
    }

    fn is_empty(&self) -> bool {
        self.freed == SEGMENT_SIZE || self.allocated == std::mem::size_of::<Segment>()
    }

    unsafe fn reset(&mut self) {
        self.allocated = std::mem::size_of::<Segment>();
        self.freed = std::mem::size_of::<Segment>();
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
                compacting: false,
                active: false,
                data: [0; 0],
            };
            debug_assert_eq!(
                segment_header_size,
                next_multiple_of(segment_header_size, RECORD_ALIGN),
            );
            let mut ptr = NonNull::new_unchecked(alloc(layout)).cast::<Segment>();
            let uninit_segment = ptr.as_uninit_mut();
            uninit_segment.write(default_segment);
            println!("allocate new segment {:X}", ptr.as_ptr() as usize);
            Box::from_raw(ptr.as_ptr())
        }
    }

    unsafe fn compact<F>(&self, migrate: F)
    where
        F: Fn(NonNull<u8>) -> usize,
    {
        let data = self as *const _ as *mut u8;
        let mut consumed = std::mem::size_of::<Segment>();
        println!(
            "try compact segment with address {:X}, freed {}MB",
            data as usize,
            self.freed / 1024 / 1024,
        );
        while consumed < SEGMENT_SIZE {
            let record_base = data.add(consumed);
            let record_size = migrate(NonNull::new_unchecked(record_base));
            if record_size == 0 {
                break;
            }
            consumed += record_size;
            consumed = next_multiple_of(consumed, RECORD_ALIGN);
        }
        println!(
            "compact segment {:X} is finished",
            self as *const _ as usize
        );
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        if !self.is_empty() {
            panic!("There are some memory leaks");
        }
    }
}

type SegmentList = LinkedList<Segment, SegmentNodeAdaptor>;

// FIXME(walter) the record meta should be atomic.
unsafe impl Send for Segment {}
unsafe impl Send for SegmentList {}

use std::collections::HashSet;

pub struct Lsa {
    waker: AtomicUsize,
    num_compacted_segments: AtomicUsize,
    allocated_segments: Mutex<HashSet<usize>>,
    active_segment: Mutex<Option<Box<Segment>>>,
    segments: Mutex<SegmentList>,
    freed_segments: Mutex<SegmentList>,
}

impl Lsa {
    pub fn new() -> Self {
        Lsa {
            waker: AtomicUsize::new(0),
            num_compacted_segments: AtomicUsize::new(0),
            allocated_segments: Mutex::new(HashSet::new()),
            active_segment: Mutex::new(None),
            segments: Mutex::new(SegmentList::new()),
            freed_segments: Mutex::new(SegmentList::new()),
        }
    }

    pub unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if Self::is_exceeds_supported_size(layout) {
            return std::alloc::alloc(layout);
        }

        for _ in 0..2 {
            {
                let mut active_seg = self.active_segment.lock().unwrap();
                if let Some(last_segment) = active_seg.as_mut() {
                    if let Some(addr) = last_segment.try_alloc(layout) {
                        return addr;
                    }

                    last_segment.active = false;
                    {
                        let mut segments = self.segments.lock().unwrap();
                        segments.push_back(active_seg.take().unwrap());
                    }
                }
            }

            self.alloc_segment();
        }

        panic!("couldn't alloc enough memory with {:?}", layout);
    }

    unsafe fn dealloc(&self, addr: *mut u8, layout: Layout) {
        if Self::is_exceeds_supported_size(layout) {
            std::alloc::dealloc(addr, layout);
            return;
        }

        // FIXME(walter) this break pointer alias rules.
        let segment_addr = addr as usize & !(SEGMENT_SIZE - 1);
        {
            let allocated_segments = self.allocated_segments.lock().unwrap();
            if !allocated_segments.contains(&segment_addr) {
                panic!("unknown segment {:X}", segment_addr);
            }
        }
        assert_ne!(segment_addr, 0);
        let mut segment_ptr = NonNull::new_unchecked(segment_addr as *mut Segment);
        let segment = segment_ptr.as_mut();
        segment.free(layout);
        if !segment.active && Self::should_compact(&segment) {
            self.wake();
        }
        if !segment.compacting {
            self.might_reuse_segment(segment_ptr);
        }
    }

    fn alloc_segment(&self) {
        let mut active_seg = self.active_segment.lock().unwrap();
        if active_seg.is_some() {
            return;
        }

        let new_segment = {
            let mut freed_segments = self.freed_segments.lock().unwrap();
            freed_segments.pop_front()
        };
        let segment = new_segment.unwrap_or_else(Segment::new);
        {
            let mut allocated_segments = self.allocated_segments.lock().unwrap();
            allocated_segments.insert(segment.as_ref() as *const _ as usize);
        }
        assert!(active_seg.is_none());
        *active_seg = Some(segment);
    }

    unsafe fn might_reuse_segment(&self, mut segment_ptr: NonNull<Segment>) {
        let segment = segment_ptr.as_mut();
        if segment.is_empty() {
            segment.reset();
            let boxed_segment = {
                let mut segments = self.segments.lock().unwrap();
                segments.unlink_node(segment_ptr)
            };

            let mut freed_segments = self.freed_segments.lock().unwrap();
            freed_segments.push_back(boxed_segment);
        }
    }

    fn is_exceeds_supported_size(layout: Layout) -> bool {
        let aligned_size = next_multiple_of(layout.size(), RECORD_ALIGN);
        aligned_size >= MAX_OBJECT_SIZE
    }

    fn compact<F>(&self, migrate: F) -> bool
    where
        F: Fn(NonNull<u8>) -> usize,
    {
        if let Some(mut boxed_segment) = self.select_segment_for_compaction() {
            unsafe {
                boxed_segment.compact(migrate);
                boxed_segment.reset();
            };

            self.num_compacted_segments.fetch_add(1, Ordering::AcqRel);
            let mut freed_segments = self.freed_segments.lock().unwrap();
            // TODO(walter) add parameter.
            if freed_segments.len() <= 1 {
                boxed_segment.compacting = false;
                freed_segments.push_back(boxed_segment);
            }
            true
        } else {
            false
        }
    }

    fn select_segment_for_compaction(&self) -> Option<Box<Segment>> {
        let mut segments = self.segments.lock().unwrap();
        // TODO(walter) add parameter.
        let limit = std::cmp::min(16, segments.len());
        for _ in 0..limit {
            if let Some(mut segment) = segments.pop_front() {
                // if the segment has free space exceeds about 20%.
                if Self::should_compact(&segment) {
                    segment.compacting = true;
                    return Some(segment);
                } else {
                    segments.push_back(segment);
                }
            }
        }
        None
    }

    fn should_compact(segment: &Segment) -> bool {
        // if the segment has free space exceeds about 20%.
        segment.freed * 100 > SEGMENT_SIZE * 20
    }

    #[inline]
    unsafe fn wake(&self) {
        let mut waker = self.waker.load(Ordering::Relaxed);
        while waker != 0 {
            waker = match self
                .waker
                .compare_exchange(waker, 0, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(waker) => {
                    std::mem::transmute::<usize, fn()>(waker)();
                    return;
                }
                Err(waker) => waker,
            };
        }
    }

    unsafe fn wait(&self, waker: fn()) {
        self.waker.store(waker as usize, Ordering::Release);
    }
}

lazy_static! {
    static ref GLOBAL_LSA: Lsa = Lsa::new();
}

#[allow(clippy::missing_safety_doc)]
pub unsafe fn lsa_alloc(layout: Layout) -> *mut u8 {
    GLOBAL_LSA.alloc(layout)
}

#[allow(clippy::missing_safety_doc)]
pub unsafe fn lsa_dealloc(addr: *mut u8, layout: Layout) {
    GLOBAL_LSA.dealloc(addr, layout);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe fn compact_segments<F>(migrate: F) -> bool
where
    F: Fn(NonNull<u8>) -> usize,
{
    GLOBAL_LSA.compact(migrate)
}

#[allow(clippy::missing_safety_doc)]
pub unsafe fn wait_for_compaction(waker: fn()) {
    GLOBAL_LSA.wait(waker);
}

#[allow(clippy::missing_safety_doc)]
pub unsafe fn wake_compaction() {
    GLOBAL_LSA.wake();
}

#[derive(Default, Debug)]
pub struct LsaMemStats {
    pub freed: usize,
    pub allocated: usize,
    pub total: usize,

    pub used_segments: usize,
    pub freed_segments: usize,
    pub compacted_segments: usize,
}

impl LsaMemStats {
    fn apply(&mut self, segment: &Segment) {
        self.freed += segment.freed;
        self.allocated += segment.allocated;
        self.total += SEGMENT_SIZE;
        self.used_segments += 1;
    }
}

#[allow(dead_code)]
pub fn read_mem_stats() -> LsaMemStats {
    let mut stats = LsaMemStats::default();
    {
        let active_segment = GLOBAL_LSA.active_segment.lock().unwrap();
        if let Some(active_segment) = active_segment.as_ref() {
            stats.apply(active_segment);
        }
    }

    {
        let segments = GLOBAL_LSA.segments.lock().unwrap();
        for segment in segments.iter() {
            stats.apply(segment);
        }
    }

    {
        let freed_segments = GLOBAL_LSA.freed_segments.lock().unwrap();
        stats.freed_segments = freed_segments.len();
    }

    stats.compacted_segments = GLOBAL_LSA.num_compacted_segments.load(Ordering::Acquire);

    stats
}

#[allow(dead_code)]
fn assert_segment_size_is_power_of_two() {
    let _: [u8; SEGMENT_SIZE] = [0; (SEGMENT_SIZE - 1).next_power_of_two()];
}
