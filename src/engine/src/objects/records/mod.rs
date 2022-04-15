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

use super::{ObjectMeta, RawObject, Tag};

#[repr(C)]
#[derive(Copy, Clone)]
pub struct RecordMeta {
    _reserved: u32,
    _reserved_1: u16,
    pub tag: Tag,
}

impl RecordMeta {
    pub fn new(tag: Tag) -> Self {
        RecordMeta {
            _reserved: 0,
            _reserved_1: 0,
            tag,
        }
    }
}

#[repr(C)]
pub struct RecordHeader {
    pub meta: RecordMeta,
}

impl RecordHeader {
    pub fn new(tag: Tag) -> Self {
        RecordHeader {
            meta: RecordMeta::new(tag),
        }
    }

    pub fn data<T>(&self) -> Option<&Record<T>>
    where
        T: RecordLayout,
    {
        unsafe {
            if self.meta.tag.contains(Tag::RECORD | T::record_type()) {
                Some(&*(self as *const RecordHeader as *const Record<T>))
            } else {
                None
            }
        }
    }
}

pub trait RecordLayout: Sized {
    fn record_type() -> Tag;

    fn layout(val: &Record<Self>) -> Layout;
}

#[repr(C)]
pub struct Record<T: RecordLayout> {
    header: RecordHeader,
    data: T,
}

impl<T: RecordLayout> Record<T> {
    pub(self) fn new(data: T) -> Self {
        Record {
            header: RecordHeader::new(T::record_type() | Tag::RECORD),
            data,
        }
    }

    pub fn tag(&self) -> Tag {
        self.header.meta.tag
    }

    /// Associated this record to a [`Object`].
    ///
    /// After that, the object can be looked up directly from the record by invoke
    /// [`associated_object`].
    pub fn associated_with(&mut self, object_meta: &ObjectMeta) {
        unsafe {
            let mut tag = self.header.meta.tag;
            tag.remove(Tag::TOMBSTONE);
            let address = object_meta as *const _ as u64;
            std::ptr::write_unaligned(self.address_mut().cast(), address);
            self.header.meta.tag = tag;
        }
    }

    /// Return the underlying associated objects
    ///
    /// # Safety
    ///
    /// User should ensure an object is associated before.
    pub unsafe fn associated_object(&self) -> RawObject {
        let mut address: u64 = std::ptr::read_unaligned(self.address().cast());
        address &= 0xFFFF_FFFF_FFFF;
        RawObject::from_raw_address(address as usize)
    }

    /// Migrate data saved by record to another record.
    ///
    /// It is designed to be used by log structured allocators.
    pub fn migrate_to(&mut self, _target: &mut Self) {
        todo!();
    }

    unsafe fn address_mut(&mut self) -> *mut u8 {
        &mut self.header as *mut _ as *mut u8
    }

    unsafe fn address(&self) -> *const u8 {
        &self.header as *const _ as *const u8
    }
}

impl<T: RecordLayout> Deref for Record<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T: RecordLayout> DerefMut for Record<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

pub struct BoxRecord<T: RecordLayout> {
    ptr: NonNull<Record<T>>,
}

impl<T: RecordLayout> BoxRecord<T> {
    pub(super) unsafe fn from_raw(ptr: NonNull<Record<T>>) -> Self {
        BoxRecord { ptr }
    }

    pub(super) fn leak(self) -> NonNull<Record<T>> {
        ManuallyDrop::new(self).ptr
    }

    pub(super) fn inner(&self) -> &NonNull<Record<T>> {
        &self.ptr
    }
}

impl<T: RecordLayout> Deref for BoxRecord<T> {
    type Target = Record<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

impl<T: RecordLayout> DerefMut for BoxRecord<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut() }
    }
}

impl<T: RecordLayout> Drop for BoxRecord<T> {
    fn drop(&mut self) {
        use std::alloc::dealloc;

        unsafe {
            let layout = T::layout(self.ptr.as_ref());

            let record = self.ptr.as_mut();
            record.header.meta.tag |= Tag::TOMBSTONE;
            std::ptr::drop_in_place(self.ptr.as_ptr());

            dealloc(self.ptr.as_ptr().cast(), layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{array::*, entry::*, list_node::*, *};

    fn show_layout<T>() {
        println!("{} {:?}", std::any::type_name::<T>(), Layout::new::<T>());
    }

    #[test]
    fn record_layout() {
        show_layout::<RecordMeta>();
        show_layout::<RecordHeader>();
        show_layout::<Array>();
        show_layout::<Record<Array>>();
        show_layout::<ListNode>();
        show_layout::<Record<ListNode>>();
        show_layout::<Entry>();
        show_layout::<Record<Entry>>();
    }

    #[test]
    fn drop_case() {
        // 1. array
        BoxRecord::<Array>::with_capacity(123);

        // 2. list node
        BoxRecord::<ListNode>::with_capacity(123);

        // 3. entry
        BoxRecord::<Entry>::with_capacity(123, 123);
    }
}
