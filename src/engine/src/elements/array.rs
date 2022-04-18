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
    ptr::NonNull,
    slice,
};

use super::{BoxElement, Element, ElementLayout, ElementType};

#[repr(C)]
pub struct Array {
    len: u32,
    pad: u32,
    data: [u8; 0],
}

impl Array {
    pub(super) fn new(len: u32) -> Self {
        Array {
            len,
            pad: 0,
            data: [],
        }
    }

    pub fn capacity(&self) -> usize {
        self.len as usize
    }

    pub fn data_slice(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(std::ptr::addr_of!(self.data) as *const _, self.len as usize)
        }
    }

    pub fn data_slice_mut(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(
                std::ptr::addr_of_mut!(self.data) as *mut _,
                self.len as usize,
            )
        }
    }
}

impl ElementLayout for Array {
    fn element_type() -> u16 {
        ElementType::ARRAY.bits
    }

    fn layout(val: &Element<Self>) -> Layout {
        let align = std::mem::align_of::<Element<Array>>();
        let fixed_size = std::mem::size_of::<Element<Array>>();
        Layout::from_size_align(fixed_size + val.data.len as usize, align).unwrap()
    }
}

impl BoxElement<Array> {
    pub fn with_capacity(size: usize) -> BoxElement<Array> {
        let align = std::mem::align_of::<Element<Array>>();
        let fixed_size = std::mem::size_of::<Element<Array>>();
        let layout = Layout::from_size_align(fixed_size + size, align).unwrap();
        unsafe {
            let mut ptr = NonNull::new_unchecked(alloc(layout) as *mut Element<Array>);
            let uninit_element = ptr.as_uninit_mut();
            uninit_element.write(Element::new(Array::new(size as u32)));
            BoxElement::from_raw(ptr)
        }
    }

    pub fn from_slice(data: &[u8]) -> BoxElement<Array> {
        let mut object = BoxElement::<Array>::with_capacity(data.len());
        object.data_slice_mut().copy_from_slice(data);
        object
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_layout() {
        let array = Array::new(123);
        println!("Array address: {:X}", std::ptr::addr_of!(array) as usize);
        println!(
            "Array::data address: {:X}",
            std::ptr::addr_of!(array.data) as usize
        );

        let element = Element::new(Array::new(123));
        println!(
            "Element<Array> address: {:X}",
            std::ptr::addr_of!(element) as usize
        );
        println!(
            "Element<Array>::data address: {:X}",
            std::ptr::addr_of!(element.data) as usize
        );
        println!(
            "Element<Array>::data.data address: {:X}",
            std::ptr::addr_of!(element.data.data) as usize
        );
    }
}
