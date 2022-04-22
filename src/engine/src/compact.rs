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

use std::ptr::NonNull;

use crate::{
    db::Db,
    elements::{array::Array, BoxElement, ElementLayout, RawElement},
    objects::{BoxObject, RawObject, RawString},
    record::*,
};

pub fn migrate_record(db: Db, record_base: NonNull<u8>) -> usize {
    // FIXME(walter) this may break the pointer alias rules if there exists a unique no-alias
    // pointer.
    let record_meta = unsafe { record_base.cast::<RecordMeta>().as_ref() };
    if record_meta.is_tombstone() {
        // Since this record has been marked as tombstone, just skip it rather than copy to new
        // address.
        return unsafe { record_size(record_meta) };
    }

    if record_meta.is_element() {
        unsafe { migrate_element(RawElement::from_raw(NonNull::from(record_meta))) }
    } else if record_meta.is_object() {
        unsafe {
            migrate_object(
                db,
                RawObject::from_raw_address(record_meta as *const _ as usize),
            )
        }
    } else {
        panic!("unknown record type");
    }
}

unsafe fn migrate_element(mut raw_element: RawElement) -> usize {
    // TODO(walter) now only `Array` is supported.
    if let Some(origin) = raw_element.data_mut::<Array>() {
        let record_size = Array::layout(origin).size();
        if let Some(mut raw_object) = origin.associated_object() {
            if let Some(raw_string) = raw_object.data_mut::<RawString>() {
                let mut target = BoxElement::<Array>::with_capacity(origin.capacity());
                target.data_slice_mut().copy_from_slice(origin.data_slice());
                raw_string.update_value(Some(target));
            } else {
                panic!("not supported object type");
            }
        } else {
            // Since this object haven't associated object, discard it.
        }
        record_size
    } else {
        panic!("not supported element type");
    }
}

unsafe fn migrate_object(db: Db, mut raw_object: RawObject) -> usize {
    // TODO(walter) now only `RawString` is supported.
    if let Some(origin) = raw_object.data_mut::<RawString>() {
        let value = origin.update_value(None);
        let new_object = BoxObject::<RawString>::with_key_value(origin.key(), value);
        let layout = new_object.object_layout();
        db.insert(new_object);
        return layout.size();
    }

    panic!("not supported object type");
}

unsafe fn record_size(record_meta: &RecordMeta) -> usize {
    if record_meta.is_element() {
        let raw_element = RawElement::from_raw(NonNull::from(record_meta));
        raw_element.element_layout().size()
    } else if record_meta.is_object() {
        let raw_object = RawObject::from_raw_address(record_meta as *const _ as usize);
        raw_object.object_layout().size()
    } else {
        panic!("unknown record type");
    }
}
