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

use std::{alloc::Layout, ptr::NonNull, slice};

use super::{BoxElement, Element, ElementLayout, ElementType};

#[repr(C)]
pub struct Entry {
    key_len: u32,
    value_len: u32,
    data: [u8; 0],
}

impl Entry {
    pub(super) fn new(key_len: u32, value_len: u32) -> Self {
        Entry {
            key_len,
            value_len,
            data: [],
        }
    }

    pub fn key_len(&self) -> usize {
        self.key_len as usize
    }

    pub fn value_len(&self) -> usize {
        self.value_len as usize
    }

    pub fn capacity(&self) -> usize {
        self.key_len() + self.value_len()
    }

    pub fn data_slice(&self) -> (&[u8], &[u8]) {
        unsafe {
            let addr = std::ptr::addr_of!(self.data) as *const u8;
            (
                slice::from_raw_parts(addr, self.key_len()),
                slice::from_raw_parts(addr.offset(self.key_len as isize), self.value_len()),
            )
        }
    }

    pub fn data_slice_mut(&mut self) -> (&mut [u8], &mut [u8]) {
        unsafe {
            let addr = std::ptr::addr_of_mut!(self.data) as *mut u8;
            (
                slice::from_raw_parts_mut(addr, self.key_len()),
                slice::from_raw_parts_mut(addr.offset(self.key_len as isize), self.value_len()),
            )
        }
    }
}

impl ElementLayout for Entry {
    fn element_type() -> u16 {
        ElementType::ENTRY.bits
    }

    fn layout(val: &Element<Self>) -> Layout {
        type Target = Element<Entry>;

        let align = std::mem::align_of::<Target>();
        let fixed_size = std::mem::size_of::<Target>();
        Layout::from_size_align(fixed_size + val.data.capacity(), align).unwrap()
    }
}

impl BoxElement<Entry> {
    pub fn with_capacity(key_len: usize, value_len: usize) -> BoxElement<Entry> {
        use std::alloc::alloc;

        type Target = Element<Entry>;

        let capacity = key_len + value_len;
        let align = std::mem::align_of::<Target>();
        let fixed_size = std::mem::size_of::<Target>();
        let layout = Layout::from_size_align(fixed_size + capacity, align).unwrap();
        unsafe {
            let ptr_addr = alloc(layout) as *mut Target;
            let mut ptr = NonNull::new_unchecked(ptr_addr);
            let uninit_element = ptr.as_uninit_mut();
            uninit_element.write(Element::new(Entry::new(key_len as u32, value_len as u32)));
            BoxElement::from_raw(ptr)
        }
    }
}
