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

pub mod hash;
pub mod list;
pub mod set;
pub mod string;

use std::{
    alloc::{alloc, Layout},
    ops::{Deref, DerefMut},
    ptr::NonNull,
    slice,
};

use bitflags::bitflags;

use crate::{
    elements::{BoxElement, Element, ElementLayout},
    objects::{hash::HashMap, list::LinkedList, set::HashSet, string::RawString},
    record::{RecordMeta, RecordType, RECORD_OBJECT},
};

bitflags! {
    #[repr(C)]
    pub struct ObjectType: u16 {
        const RAW_STRING = 0x1 << 0;
        const LINKED_LIST = 0x1 << 1;
        const HASH_TABLE = 0x1 << 2;
        const SET = 0x1 << 3;
    }
}

#[repr(C)]
pub struct ObjectMeta {
    pub meta: RecordMeta,
    deadline: u64,
}

impl ObjectMeta {
    fn new(meta: RecordMeta) -> Self {
        ObjectMeta { meta, deadline: 0 }
    }

    pub fn deadline(&self) -> u64 {
        self.deadline
    }

    pub fn set_deadline(&mut self, val: u64) {
        self.deadline = val;
    }

    pub fn clear_deadline(&mut self) {
        self.deadline = u64::MAX;
    }

    pub fn object_type(&self) -> u16 {
        self.meta.user_defined_tag()
    }

    pub fn key_len(&self) -> usize {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.meta.left[2..]);
        u32::from_le_bytes(bytes) as usize
    }

    pub fn set_tombstone(&mut self) {
        self.meta.set_tombstone();
    }
}

#[repr(C)]
pub struct Object<T: ObjectLayout> {
    pub meta: ObjectMeta,
    value: T,
    key: [u8; 0],
}

impl<T: ObjectLayout> Object<T> {
    pub(self) fn new(key_len: usize, value: T) -> Self {
        let key_len: u32 = key_len as u32;
        let mut meta = RecordMeta::object(T::object_type());
        meta.left[..2].copy_from_slice(0u16.to_le_bytes().as_ref());
        meta.left[2..].copy_from_slice(key_len.to_le_bytes().as_ref());
        Object {
            meta: ObjectMeta::new(meta),
            value,
            key: [],
        }
    }

    pub fn key(&self) -> &'static [u8] {
        unsafe {
            slice::from_raw_parts(
                std::ptr::addr_of!(self.key) as *const u8,
                self.meta.key_len() as usize,
            )
        }
    }

    /// Migrate this object to new place.
    ///
    /// # Safety
    ///
    /// TODO(walter)
    pub unsafe fn migrate_to(&mut self, _target: &mut Self) {
        todo!()
    }

    /// Migrate this associated records to new place.
    ///
    /// # Safety
    ///
    /// TODO(walter)
    pub unsafe fn migrate_record<R>(&mut self, _target: BoxElement<R>, _src: &Element<R>)
    where
        R: ElementLayout,
    {
        todo!()
    }
}

impl<T: ObjectLayout> RecordType for Object<T> {
    fn record_type() -> u16 {
        RECORD_OBJECT
    }
}

impl<T: ObjectLayout> Deref for Object<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T: ObjectLayout> DerefMut for Object<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub trait ObjectLayout: Sized {
    fn object_type() -> u16;
}

#[derive(Clone, Copy)]
pub struct RawObject {
    ptr: NonNull<ObjectMeta>,
}

impl RawObject {
    /// Build `RawObject` from raw address.
    ///
    /// # Safety
    ///
    /// `address` must be a valid address of `Object`.
    pub unsafe fn from_raw_address(address: usize) -> Self {
        RawObject {
            ptr: NonNull::new_unchecked(address as *mut _),
        }
    }

    /// Build `RawObject` from raw ptr.
    ///
    /// # Safety
    ///
    /// TODO(walter)
    pub unsafe fn from_raw(ptr: NonNull<ObjectMeta>) -> Self {
        RawObject { ptr }
    }

    /// # Safety
    ///
    /// TODO(walter)
    unsafe fn object_meta(&self) -> &ObjectMeta {
        self.ptr.as_ref()
    }

    /// # Safety
    ///
    /// TODO(walter)
    #[allow(dead_code)]
    unsafe fn object_meta_mut(&mut self) -> &mut ObjectMeta {
        self.ptr.as_mut()
    }

    /// # Safety
    ///
    /// TODO(walter)
    unsafe fn as_ref<T>(&self) -> &T {
        &*(self.ptr.as_ptr() as *const T)
    }

    /// # Safety
    ///
    /// TODO(walter)
    unsafe fn as_mut<'a, T>(&self) -> &'a mut T {
        &mut *(self.ptr.as_ptr() as *mut T)
    }
}

impl RawObject {
    pub fn data<T>(&self) -> Option<&Object<T>>
    where
        T: ObjectLayout,
    {
        unsafe {
            let meta = self.object_meta();
            if T::object_type() == meta.object_type() {
                Some(self.as_ref())
            } else {
                None
            }
        }
    }

    pub fn data_mut<T>(&mut self) -> Option<&mut Object<T>>
    where
        T: ObjectLayout,
    {
        unsafe {
            let meta = self.object_meta();
            if T::object_type() == meta.object_type() {
                Some(self.as_mut())
            } else {
                None
            }
        }
    }

    pub fn key(&self) -> &'static [u8] {
        const RAW_STRING: u16 = ObjectType::RAW_STRING.bits;
        const LINKED_LIST: u16 = ObjectType::LINKED_LIST.bits;
        const HASH_TABLE: u16 = ObjectType::HASH_TABLE.bits;
        const SET: u16 = ObjectType::SET.bits;

        unsafe {
            let meta = self.object_meta();
            match meta.object_type() {
                RAW_STRING => self.as_ref::<Object<RawString>>().key(),
                LINKED_LIST => self.as_ref::<Object<LinkedList>>().key(),
                HASH_TABLE => self.as_ref::<Object<HashMap>>().key(),
                SET => self.as_ref::<Object<HashSet>>().key(),
                v => panic!("unknown object type {}", v),
            }
        }
    }

    pub fn drop_in_place(self) {
        const RAW_STRING: u16 = ObjectType::RAW_STRING.bits;
        const LINKED_LIST: u16 = ObjectType::LINKED_LIST.bits;
        const HASH_TABLE: u16 = ObjectType::HASH_TABLE.bits;
        const SET: u16 = ObjectType::SET.bits;

        unsafe {
            let meta = self.object_meta();
            match meta.object_type() {
                RAW_STRING => {
                    BoxObject::from_raw(self.ptr.cast::<Object<RawString>>());
                }
                LINKED_LIST => {
                    BoxObject::from_raw(self.ptr.cast::<Object<LinkedList>>());
                }
                HASH_TABLE => {
                    BoxObject::from_raw(self.ptr.cast::<Object<HashMap>>());
                }
                SET => {
                    BoxObject::from_raw(self.ptr.cast::<Object<HashSet>>());
                }
                v => panic!("unknown object type {}", v),
            }
        }
    }
}

pub struct BoxObject<T: ObjectLayout> {
    ptr: NonNull<Object<T>>,
}

impl<T: ObjectLayout> BoxObject<T> {
    pub fn with_key(key: &[u8]) -> Self
    where
        T: Default,
    {
        use std::ptr::copy_nonoverlapping;

        let size = std::mem::size_of::<Object<T>>() + key.len();
        let align = std::mem::align_of::<Object<T>>();
        let layout = Layout::from_size_align(size, align).unwrap();

        unsafe {
            let mut ptr = NonNull::new_unchecked(alloc(layout).cast::<Object<T>>());
            let uninit_object = ptr.as_uninit_mut();
            uninit_object.write(Object::new(key.len(), T::default()));

            let mut object = BoxObject::from_raw(ptr);
            copy_nonoverlapping(
                key.as_ptr(),
                std::ptr::addr_of_mut!(object.key) as *mut u8,
                key.len(),
            );
            object
        }
    }

    pub(self) unsafe fn from_raw(ptr: NonNull<Object<T>>) -> Self {
        BoxObject { ptr }
    }

    pub fn leak(self) -> RawObject {
        use std::mem::ManuallyDrop;
        unsafe {
            let ptr = ManuallyDrop::new(self).ptr;
            RawObject::from_raw(ptr.cast::<ObjectMeta>())
        }
    }
}

impl<T: ObjectLayout> Deref for BoxObject<T> {
    type Target = Object<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

impl<T: ObjectLayout> DerefMut for BoxObject<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut() }
    }
}

impl<T: ObjectLayout> Drop for BoxObject<T> {
    fn drop(&mut self) {
        use std::alloc::dealloc;

        unsafe {
            let key_len = self.meta.key_len() as usize;
            let fixed_size = std::mem::size_of::<Object<T>>();
            let align = std::mem::align_of::<Object<T>>();
            let layout = Layout::from_size_align(fixed_size + key_len, align).unwrap();

            let object = self.ptr.as_mut();
            object.meta.set_tombstone();
            std::ptr::drop_in_place(self.ptr.as_ptr());

            dealloc(self.ptr.as_ptr().cast(), layout);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{list::*, string::*, *};
    use crate::elements::array::*;

    fn show_layout<T>() {
        println!("{} {:?}", std::any::type_name::<T>(), Layout::new::<T>());
    }

    #[test]
    fn object_layout() {
        show_layout::<ObjectMeta>();
        show_layout::<Object<RawString>>();
        show_layout::<Object<LinkedList>>();
    }

    #[test]
    fn object_key() {
        let obj = BoxObject::<RawString>::with_key(&[0]);
        assert_eq!(obj.key(), &[0]);

        let obj = BoxObject::<RawString>::with_key(&[0, 1, 2, 3, 4, 5]);
        assert_eq!(obj.key(), &[0, 1, 2, 3, 4, 5]);

        let key = (0..1000).into_iter().map(|i| i as u8).collect::<Vec<_>>();
        let obj = BoxObject::<RawString>::with_key(&key);
        assert_eq!(obj.key(), key);
    }

    #[test]
    fn object_drop_case() {
        // 1. RawString
        let obj = BoxObject::<RawString>::with_key(&[0]);
        drop(obj);

        // 2. RawString with Array
        let mut obj = BoxObject::<RawString>::with_key(&[0, 1, 2, 3]);
        let mut array = BoxElement::<Array>::with_capacity(5);
        array.data_slice_mut().copy_from_slice(&[0, 1, 2, 3, 4]);
        obj.update_value(array);
        drop(obj);

        // 3. RawString replace value
        let mut obj = BoxObject::<RawString>::with_key(&[0, 1, 2, 3]);
        let mut array = BoxElement::<Array>::with_capacity(5);
        array.data_slice_mut().copy_from_slice(&[0, 1, 2, 3, 4]);
        obj.update_value(array);
        let mut new_array = BoxElement::<Array>::with_capacity(3);
        new_array.data_slice_mut().copy_from_slice(&[0, 1, 2]);
        obj.update_value(new_array);
        drop(obj);
    }
}
