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

use super::{
    records::{list_node::ListNode, BoxRecord, Record},
    Object, ObjectType, ObjectVTable, Tag,
};
use crate::object_vtable;

object_vtable!(LinkedList, LINKED_LIST_VTABLE);

#[repr(C)]
#[derive(Default)]
pub struct LinkedList {
    head: Option<NonNull<Record<ListNode>>>,
    tail: Option<NonNull<Record<ListNode>>>,
    len: u32,
    pad: u32,
}

impl Object<LinkedList> {
    pub fn pop_front(&mut self) -> Option<BoxRecord<ListNode>> {
        if let Some(node) = self.head {
            let mut node = unsafe { BoxRecord::from_raw(node) };
            if let Some(mut next) = node.next.take() {
                unsafe { next.as_mut().prev = None };
                self.head = Some(next);
            } else {
                self.tail = None;
                self.head = None;
            }
            self.len -= 1;
            assert!(node.prev.is_none());
            Some(node)
        } else {
            None
        }
    }

    pub fn push_front(&mut self, mut node: BoxRecord<ListNode>) {
        if let Some(first) = self.head.as_mut() {
            unsafe {
                first.as_mut().prev = Some(*node.inner());
            }
        }
        node.associated_with(&self.meta);
        node.next = self.head;
        node.prev = None;
        self.head = Some(BoxRecord::leak(node));
        if self.tail.is_none() {
            self.tail = self.head;
        }
        self.len += 1;
    }

    pub fn pop_back(&mut self) -> Option<BoxRecord<ListNode>> {
        if let Some(node) = self.tail {
            let mut node = unsafe { BoxRecord::from_raw(node) };
            if let Some(mut prev) = node.prev.take() {
                unsafe { prev.as_mut().next = None };
                self.tail = Some(prev);
            } else {
                self.head = None;
                self.tail = None;
            }
            self.len -= 1;
            assert!(node.next.is_none());
            Some(node)
        } else {
            None
        }
    }

    pub fn push_back(&mut self, mut node: BoxRecord<ListNode>) {
        if let Some(last) = self.tail.as_mut() {
            unsafe {
                last.as_mut().next = Some(*node.inner());
            }
        }
        node.associated_with(&self.meta);
        node.next = None;
        node.prev = self.tail;
        self.tail = Some(BoxRecord::leak(node));
        if self.head.is_none() {
            self.head = self.tail;
        }
        self.len += 1;
    }

    pub fn iter(&self) -> impl Iterator<Item = &Record<ListNode>> {
        LinkedListIterator::new(self)
    }

    pub fn iter_mut(&self) -> impl Iterator<Item = &mut Record<ListNode>> {
        LinkedListIteratorMut::new(self)
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl ObjectType for LinkedList {
    fn object_type() -> Tag {
        Tag::LINKED_LIST
    }

    fn vtable() -> &'static ObjectVTable {
        &LINKED_LIST_VTABLE
    }
}

impl Drop for LinkedList {
    fn drop(&mut self) {
        if let Some(node) = self.head.take() {
            let mut node = unsafe { BoxRecord::from_raw(node) };
            while let Some(next) = node.next.take() {
                node.prev = None;
                node = unsafe { BoxRecord::from_raw(next) };
            }
            node.prev = None;
        }
    }
}

struct LinkedListIterator<'a> {
    _list: &'a LinkedList,
    next: Option<NonNull<Record<ListNode>>>,
}

impl<'a> LinkedListIterator<'a> {
    fn new(list: &'a LinkedList) -> Self {
        let next = list.head;
        LinkedListIterator { _list: list, next }
    }
}

impl<'a> Iterator for LinkedListIterator<'a> {
    type Item = &'a Record<ListNode>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(node) = self.next.take() {
            unsafe {
                let node = node.as_ref();
                self.next = node.next;
                Some(node)
            }
        } else {
            None
        }
    }
}

struct LinkedListIteratorMut<'a> {
    _list: &'a LinkedList,
    next: Option<NonNull<Record<ListNode>>>,
}

impl<'a> LinkedListIteratorMut<'a> {
    fn new(list: &'a LinkedList) -> Self {
        let next = list.head;
        LinkedListIteratorMut { _list: list, next }
    }
}

impl<'a> Iterator for LinkedListIteratorMut<'a> {
    type Item = &'a mut Record<ListNode>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut node) = self.next {
            unsafe {
                let node = node.as_mut();
                self.next = node.next;
                Some(node)
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objects::BoxObject;

    #[test]
    fn linked_list() {
        let values = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8];

        // push back
        let mut list: BoxObject<LinkedList> = BoxObject::with_key(&[1, 2, 3]);
        for value in &values {
            let mut node = BoxRecord::<ListNode>::with_capacity(1);
            node.data_slice_mut()[0] = *value;
            list.push_back(node);
        }
        assert_eq!(list.len(), values.len());

        let got_values = list
            .iter()
            .map(|node| node.data_slice()[0])
            .collect::<Vec<_>>();
        assert_eq!(values, got_values);

        // push front
        let mut list: BoxObject<LinkedList> = BoxObject::with_key(&[1, 2, 3]);
        for value in values.iter().rev() {
            let mut node = BoxRecord::<ListNode>::with_capacity(1);
            node.data_slice_mut()[0] = *value;
            list.push_front(node);
        }
        assert_eq!(list.len(), values.len());

        let got_values = list
            .iter()
            .map(|node| node.data_slice()[0])
            .collect::<Vec<_>>();
        assert_eq!(values, got_values);

        // pop front
        let mut list: BoxObject<LinkedList> = BoxObject::with_key(&[1, 2, 3]);
        for value in &values {
            let mut node = BoxRecord::<ListNode>::with_capacity(1);
            node.data_slice_mut()[0] = *value;
            list.push_back(node);
        }
        assert_eq!(list.len(), values.len());

        let mut got_values = vec![];
        while let Some(node) = list.pop_front() {
            got_values.push(node.data_slice()[0]);
        }
        assert_eq!(values, got_values);

        // pop back
        let mut list: BoxObject<LinkedList> = BoxObject::with_key(&[1, 2, 3]);
        for value in values.iter().rev() {
            let mut node = BoxRecord::<ListNode>::with_capacity(1);
            node.data_slice_mut()[0] = *value;
            list.push_back(node);
        }
        assert_eq!(list.len(), values.len());

        let mut got_values = vec![];
        while let Some(node) = list.pop_back() {
            got_values.push(node.data_slice()[0]);
        }
        assert_eq!(values, got_values);
    }
}
