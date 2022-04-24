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

pub mod array;
pub mod entry;
pub mod list_node;

use std::{
    alloc::Layout,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use bitflags::bitflags;

use self::{array::Array, entry::Entry, list_node::ListNode};
use crate::{
    objects::{ObjectMeta, RawObject},
    record::{RecordMeta, RecordType, RECORD_ELEMENT},
};

bitflags! {
    #[repr(C)]
    pub struct ElementType: u16 {
        const ARRAY = 0x1 << 0;
        const ENTRY = 0x1 << 1;
        const LIST_NODE = 0x1 << 2;
    }
}

pub trait ElementLayout: Sized {
    fn element_type() -> u16;

    fn layout(val: &Element<Self>) -> Layout;
}

#[repr(C)]
pub struct Element<T: ElementLayout> {
    meta: RecordMeta,
    data: T,
}

impl<T: ElementLayout> Element<T> {
    pub(super) fn new(data: T) -> Self {
        Element {
            meta: RecordMeta::element(T::element_type()),
            data,
        }
    }

    // pub fn tag(&self) -> Tag {
    //     self.header.meta.tag
    // }

    /// Associated this element to a [`Object`].
    ///
    /// After that, the object can be looked up directly from the element by invoke
    /// [`associated_object`].
    pub fn associated_with(&mut self, object_meta: &ObjectMeta) {
        self.meta.clear_tombstone();
        let address = object_meta as *const _ as u64;
        self.meta.left.copy_from_slice(&address.to_le_bytes()[..6]);
    }

    /// Return the underlying associated objects
    ///
    /// # Safety
    ///
    /// User should ensure an object is associated before.
    pub unsafe fn associated_object(&self) -> Option<RawObject> {
        let mut bytes = [0u8; 8];
        bytes[..6].copy_from_slice(&self.meta.left[..]);
        let address = u64::from_le_bytes(bytes);
        if address != 0 {
            Some(RawObject::from_raw_address(address as usize))
        } else {
            None
        }
    }

    /// Migrate data saved by element to another element.
    ///
    /// It is designed to be used by log structured allocators.
    pub fn migrate_to(&mut self, _target: &mut Self) {
        todo!();
    }
}

impl<T: ElementLayout> RecordType for Element<T> {
    fn record_type() -> u16 {
        RECORD_ELEMENT
    }
}

impl<T: ElementLayout> Deref for Element<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T: ElementLayout> DerefMut for Element<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

pub struct BoxElement<T: ElementLayout> {
    ptr: NonNull<Element<T>>,
}

impl<T: ElementLayout> BoxElement<T> {
    pub(super) unsafe fn from_raw(ptr: NonNull<Element<T>>) -> Self {
        BoxElement { ptr }
    }

    pub(super) fn leak(self) -> NonNull<Element<T>> {
        ManuallyDrop::new(self).ptr
    }

    pub(super) fn inner(&self) -> &NonNull<Element<T>> {
        &self.ptr
    }
}

impl<T: ElementLayout> Deref for BoxElement<T> {
    type Target = Element<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

impl<T: ElementLayout> DerefMut for BoxElement<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut() }
    }
}

impl<T: ElementLayout> Drop for BoxElement<T> {
    fn drop(&mut self) {
        // use std::alloc::dealloc;
        use crate::alloc::lsa_dealloc as dealloc;

        unsafe {
            let layout = T::layout(self.ptr.as_ref());

            let element = self.ptr.as_mut();
            element.meta.set_tombstone();
            std::ptr::drop_in_place(self.ptr.as_ptr());

            dealloc(self.ptr.as_ptr().cast(), layout);
        }
    }
}

#[repr(C)]
pub struct RawElement {
    pub record_meta: NonNull<RecordMeta>,
}

impl RawElement {
    pub fn from_raw(record_meta: NonNull<RecordMeta>) -> Self {
        RawElement { record_meta }
    }

    pub unsafe fn element_layout(&self) -> Layout {
        const ARRAY: u16 = ElementType::ARRAY.bits();
        const ENTRY: u16 = ElementType::ENTRY.bits();
        const LIST_NODE: u16 = ElementType::LIST_NODE.bits();
        let record_meta = self.record_meta.as_ref();
        match record_meta.user_defined_tag() {
            ARRAY => Array::layout(self.record_meta.cast().as_ref()),
            ENTRY => Entry::layout(self.record_meta.cast().as_ref()),
            LIST_NODE => ListNode::layout(self.record_meta.cast().as_ref()),
            v => panic!(
                "unknown element {}, raw tag is {:b}, address {:X}",
                v,
                record_meta.tag(),
                self.record_meta.as_ptr() as usize
            ),
        }
    }

    pub unsafe fn data_mut<T>(&mut self) -> Option<&mut Element<T>>
    where
        T: ElementLayout,
    {
        if T::element_type() == self.element_type() {
            Some(self.record_meta.cast().as_mut())
        } else {
            None
        }
    }

    pub unsafe fn element_type(&self) -> u16 {
        self.record_meta.as_ref().user_defined_tag()
    }
}

#[cfg(test)]
mod tests {
    use super::{array::*, entry::*, list_node::*, *};

    fn show_layout<T>() {
        println!("{} {:?}", std::any::type_name::<T>(), Layout::new::<T>());
    }

    #[test]
    fn element_layout() {
        show_layout::<RecordMeta>();
        show_layout::<Array>();
        show_layout::<Element<Array>>();
        show_layout::<ListNode>();
        show_layout::<Element<ListNode>>();
        show_layout::<Entry>();
        show_layout::<Element<Entry>>();
    }

    #[test]
    fn drop_case() {
        // 1. array
        BoxElement::<Array>::with_capacity(123);

        // 2. list node
        BoxElement::<ListNode>::with_capacity(123);

        // 3. entry
        BoxElement::<Entry>::with_capacity(123, 123);
    }
}
