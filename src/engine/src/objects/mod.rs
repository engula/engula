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
pub mod records;
pub mod set;
pub mod string;

use std::{
    alloc::{alloc, Layout},
    ops::{Deref, DerefMut},
    ptr::NonNull,
    slice,
};

use bitflags::bitflags;

use self::records::{BoxRecord, Record, RecordLayout};

bitflags! {
    // 0: record or object
    // 1: tombstone
    // 2: reserved
    // 3 - 15: type
    #[repr(C)]
    pub struct Tag : u16 {
        const RAW_STRING = 1 << 3;
        const LINKED_LIST = 2 << 3;
        const HASH_TABLE = 3 << 3;
        const SET = 4 << 4;

        const RECORD_ARRAY = 1 << 3;
        const RECORD_LIST_NODE = 2 << 3;
        const RECORD_ENTRY = 3 << 3;

        // object or record
        const RECORD = 0;
        const OBJECT = 1;
        // This object or record has been deleted.
        const TOMBSTONE = 2;
        const TAIL_MASK = 0b111;
    }
}

impl Tag {
    fn r#type(self) -> u16 {
        self.bits & 0b1
    }

    pub fn is_record(self) -> bool {
        self.r#type() == Self::RECORD.bits
    }

    pub fn is_object(self) -> bool {
        self.r#type() == Self::OBJECT.bits
    }

    fn content_type(self) -> Tag {
        unsafe { Tag::from_bits_unchecked(self.bits & !Self::TAIL_MASK.bits) }
    }

    pub fn is_raw_string(self) -> bool {
        self.is_object() && self.content_type() == Self::RAW_STRING
    }

    pub fn is_linked_list(self) -> bool {
        self.is_object() && self.content_type() == Self::LINKED_LIST
    }

    pub fn is_hash_map(self) -> bool {
        self.is_object() && self.content_type() == Self::HASH_TABLE
    }

    pub fn is_hash_set(self) -> bool {
        self.is_object() && self.content_type() == Self::SET
    }

    pub fn is_record_array(self) -> bool {
        self.is_record() && self.content_type() == Self::RECORD_ARRAY
    }

    pub fn is_record_entry(self) -> bool {
        self.is_record() && self.content_type() == Self::RECORD_ENTRY
    }

    pub fn is_record_list_node(self) -> bool {
        self.is_record() && self.content_type() == Self::RECORD_LIST_NODE
    }
}

#[repr(C)]
pub struct ObjectMeta {
    pub key_len: u32,
    pub lru: u16,
    pub tag: Tag,
    pub deadline: u64,
}

impl ObjectMeta {
    fn new(object_type: Tag, key_len: usize) -> Self {
        ObjectMeta {
            key_len: key_len as u32,
            lru: 0,
            tag: Tag::OBJECT | object_type,
            deadline: u64::MAX,
        }
    }

    pub fn object_type(&self) -> Tag {
        self.tag.content_type()
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
}

#[repr(C)]
pub struct Object<T: ObjectType> {
    pub meta: ObjectMeta,
    value: T,
    key: [u8; 0],
}

impl<T: ObjectType> Object<T> {
    pub(self) fn new(key_len: usize, value: T) -> Self {
        Object {
            meta: ObjectMeta::new(T::object_type(), key_len),
            value,
            key: [],
        }
    }

    pub fn key(&self) -> &'static [u8] {
        unsafe {
            slice::from_raw_parts(
                std::ptr::addr_of!(self.key) as *const u8,
                self.meta.key_len as usize,
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
    pub unsafe fn migrate_record<R>(&mut self, _target: BoxRecord<R>, _src: &Record<R>)
    where
        R: RecordLayout,
    {
        todo!()
    }
}

impl<T: ObjectType> Deref for Object<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T: ObjectType> DerefMut for Object<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub trait ObjectType: Sized {
    fn object_type() -> Tag;
    fn vtable() -> &'static ObjectVTable;
}

pub struct ObjectVTable {
    key: unsafe fn(*const ()) -> &'static [u8],

    drop_in_place: unsafe fn(*const ()),
}

#[derive(Clone)]
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

#[derive(Clone)]
pub struct ObjectRef {
    raw: RawObject,
    vtable: &'static ObjectVTable,
}

impl ObjectRef {
    pub fn key(&self) -> &'static [u8] {
        unsafe { (self.vtable.key)(self.raw.ptr.as_ptr().cast()) }
    }

    pub fn data<T>(&self) -> Option<&Object<T>>
    where
        T: ObjectType,
    {
        unsafe {
            let meta = self.raw.object_meta();
            if T::object_type() == meta.object_type() {
                Some(self.raw.as_ref())
            } else {
                None
            }
        }
    }

    pub fn data_mut<T>(&mut self) -> Option<&mut Object<T>>
    where
        T: ObjectType,
    {
        unsafe {
            let meta = self.raw.object_meta();
            if T::object_type() == meta.object_type() {
                Some(self.raw.as_mut())
            } else {
                None
            }
        }
    }

    pub fn drop_in_place(self) {
        unsafe { (self.vtable.drop_in_place)(self.raw.ptr.as_ptr().cast()) }
    }
}

pub struct BoxObject<T: ObjectType> {
    ptr: NonNull<Object<T>>,
}

impl<T: ObjectType> BoxObject<T> {
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

    pub fn leak(self) -> ObjectRef {
        use std::mem::ManuallyDrop;
        unsafe {
            let ptr = ManuallyDrop::new(self).ptr;
            ObjectRef {
                raw: RawObject::from_raw(ptr.cast::<ObjectMeta>()),
                vtable: T::vtable(),
            }
        }
    }
}

impl<T: ObjectType> Deref for BoxObject<T> {
    type Target = Object<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

impl<T: ObjectType> DerefMut for BoxObject<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut() }
    }
}

impl<T: ObjectType> Drop for BoxObject<T> {
    fn drop(&mut self) {
        use std::alloc::dealloc;

        unsafe {
            let key_len = self.meta.key_len as usize;
            let fixed_size = std::mem::size_of::<Object<T>>();
            let align = std::mem::align_of::<Object<T>>();
            let layout = Layout::from_size_align(fixed_size + key_len, align).unwrap();

            let object = self.ptr.as_mut();
            object.meta.tag |= Tag::TOMBSTONE;
            std::ptr::drop_in_place(self.ptr.as_ptr());

            dealloc(self.ptr.as_ptr().cast(), layout);
        }
    }
}

unsafe fn raw_object_key<T: ObjectType>(ptr: *const ()) -> &'static [u8] {
    let ptr = NonNull::new_unchecked(ptr as *mut Object<T>);
    ptr.as_ref().key()
}

unsafe fn raw_object_drop_in_place<T: ObjectType>(ptr: *const ()) {
    BoxObject::from_raw(NonNull::new_unchecked(ptr as *mut Object<T>));
}

#[macro_export]
macro_rules! object_vtable {
    ($object_type:ty, $name:ident) => {
        use crate::objects::{raw_object_drop_in_place, raw_object_key};

        static $name: ObjectVTable = ObjectVTable {
            key: raw_object_key::<$object_type>,
            drop_in_place: raw_object_drop_in_place::<$object_type>,
        };
    };
}

#[cfg(test)]
mod tests {

    use super::{
        list::*,
        records::{array::*, *},
        string::*,
        *,
    };

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
        let mut array = BoxRecord::<Array>::with_capacity(5);
        array.data_slice_mut().copy_from_slice(&[0, 1, 2, 3, 4]);
        obj.update_value(array);
        drop(obj);

        // 3. RawString replace value
        let mut obj = BoxObject::<RawString>::with_key(&[0, 1, 2, 3]);
        let mut array = BoxRecord::<Array>::with_capacity(5);
        array.data_slice_mut().copy_from_slice(&[0, 1, 2, 3, 4]);
        obj.update_value(array);
        let mut new_array = BoxRecord::<Array>::with_capacity(3);
        new_array.data_slice_mut().copy_from_slice(&[0, 1, 2]);
        obj.update_value(new_array);
        drop(obj);
    }
}
