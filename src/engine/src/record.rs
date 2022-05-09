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

pub const RECORD_ELEMENT: u16 = 0b0000;
pub const RECORD_OBJECT: u16 = 0b0010;
const RECORD_USED_MASK: u16 = 0b0001;
const RECORD_TYPE_MASK: u16 = 0b0010;
const RECORD_TOMBSTONE: u16 = 0b0100;
const RECORD_EVICTED: u16 = 0b1000;
const RECORD_META_SHIFT: u32 = 4;

pub trait RecordType {
    fn record_type() -> u16;
}

#[repr(C, align(8))]
#[derive(Clone, Copy, Default)]
pub struct RecordMeta {
    /// Hold the meta tag of the record.
    ///
    /// bits:
    /// - 0: is this memory used?
    /// - 1: object or element
    /// - 2: tombstone?
    /// - 3: evicted?
    /// - 4-15: object or element defined
    tag: u16,
    left: [u8; 6],
}

impl RecordMeta {
    pub fn element(tag: u16) -> Self {
        let tag = tag
            .checked_shl(RECORD_META_SHIFT)
            .expect("user defined tag out of range");
        RecordMeta {
            tag: tag | RECORD_ELEMENT | RECORD_USED_MASK,
            left: [0; 6],
        }
    }

    pub fn object(tag: u16) -> Self {
        let tag = tag
            .checked_shl(RECORD_META_SHIFT)
            .expect("user defined tag out of range");
        RecordMeta {
            tag: tag | RECORD_OBJECT | RECORD_USED_MASK,
            left: [0; 6],
        }
    }

    pub fn as_ref<'a, T>(&self) -> Option<&'a T>
    where
        T: RecordType,
    {
        if T::record_type() == self.record_type() {
            Some(unsafe { &*(self as *const RecordMeta as *const T) })
        } else {
            None
        }
    }

    pub fn as_mut<'a, T>(&self) -> Option<&'a mut T>
    where
        T: RecordType,
    {
        if T::record_type() == self.record_type() {
            Some(unsafe { &mut *(self as *const RecordMeta as *mut RecordMeta as *mut T) })
        } else {
            None
        }
    }

    pub fn is_object(&self) -> bool {
        self.record_type() == RECORD_OBJECT
    }

    pub fn is_element(&self) -> bool {
        self.record_type() == RECORD_ELEMENT
    }

    pub fn user_defined_tag(&self) -> u16 {
        self.tag.wrapping_shr(RECORD_META_SHIFT)
    }

    pub fn is_tombstone(self) -> bool {
        self.tag & RECORD_TOMBSTONE != 0
    }

    pub fn set_tombstone(&mut self) {
        self.tag |= RECORD_TOMBSTONE;
    }

    pub fn clear_tombstone(&mut self) {
        self.tag &= !RECORD_TOMBSTONE;
    }

    pub fn is_evicted(self) -> bool {
        self.tag & RECORD_EVICTED != 0
    }

    pub fn set_evicted(&mut self) {
        self.tag |= RECORD_EVICTED;
    }

    pub fn clear_evicted(&mut self) {
        self.tag &= !RECORD_EVICTED;
    }

    pub fn tag(&self) -> u16 {
        self.tag
    }

    fn record_type(&self) -> u16 {
        self.tag & RECORD_TYPE_MASK
    }
}

impl RecordMeta {
    pub(crate) fn set_key_len(&mut self, key_len: u32) {
        let bytes = key_len.to_le_bytes();
        self.left[3..].copy_from_slice(&bytes[..3]);
    }

    pub(crate) fn key_len(&self) -> u32 {
        let mut bytes = [0u8; 4];
        bytes[0..3].copy_from_slice(&self.left[3..]);
        u32::from_le_bytes(bytes)
    }

    pub fn set_lru(&mut self, val: u32) {
        self.left[0..3].copy_from_slice(&val.to_le_bytes()[0..3]);
    }

    pub fn lru(&self) -> u32 {
        let mut bytes = [0u8; 4];
        bytes[0..3].copy_from_slice(&self.left[..3]);
        u32::from_le_bytes(bytes)
    }
}

impl RecordMeta {
    pub fn set_associated_address(&mut self, address: usize) {
        self.left.copy_from_slice(&address.to_le_bytes()[..6]);
    }

    pub fn associated_address(&self) -> usize {
        let mut bytes = [0u8; 8];
        bytes[..6].copy_from_slice(&self.left[..]);
        usize::from_le_bytes(bytes)
    }
}

#[allow(dead_code)]
fn assert_address_space() {
    // sizeof(usize) == sizeof(u64)
    let _: [u8; std::mem::size_of::<u64>()] = [0; std::mem::size_of::<usize>()];
}
