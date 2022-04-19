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

use super::{Object, ObjectLayout, ObjectType};
use crate::elements::{list_node::ListNode, BoxElement, Element};

#[repr(C)]
#[derive(Default)]
pub struct LinkedList {
    head: Option<NonNull<Element<ListNode>>>,
    tail: Option<NonNull<Element<ListNode>>>,
    len: u32,
    pad: u32,
}

impl Object<LinkedList> {
    pub fn pop_front(&mut self) -> Option<BoxElement<ListNode>> {
        if let Some(node) = self.head {
            let mut node = unsafe { BoxElement::from_raw(node) };
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

    pub fn push_front(&mut self, mut node: BoxElement<ListNode>) {
        if let Some(first) = self.head.as_mut() {
            unsafe {
                first.as_mut().prev = Some(*node.inner());
            }
        }
        node.associated_with(&self.meta);
        node.next = self.head;
        node.prev = None;
        self.head = Some(BoxElement::leak(node));
        if self.tail.is_none() {
            self.tail = self.head;
        }
        self.len += 1;
    }

    pub fn pop_back(&mut self) -> Option<BoxElement<ListNode>> {
        if let Some(node) = self.tail {
            let mut node = unsafe { BoxElement::from_raw(node) };
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

    pub fn push_back(&mut self, mut node: BoxElement<ListNode>) {
        if let Some(last) = self.tail.as_mut() {
            unsafe {
                last.as_mut().next = Some(*node.inner());
            }
        }
        node.associated_with(&self.meta);
        node.next = None;
        node.prev = self.tail;
        self.tail = Some(BoxElement::leak(node));
        if self.head.is_none() {
            self.head = self.tail;
        }
        self.len += 1;
    }

    pub fn iter(&self) -> impl Iterator<Item = &Element<ListNode>> {
        LinkedListIterator::new(self)
    }

    pub fn iter_mut(&self) -> impl Iterator<Item = &mut Element<ListNode>> {
        LinkedListIteratorMut::new(self)
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl ObjectLayout for LinkedList {
    fn object_type() -> u16 {
        ObjectType::LINKED_LIST.bits
    }
}

impl Drop for LinkedList {
    fn drop(&mut self) {
        if let Some(node) = self.head.take() {
            let mut node = unsafe { BoxElement::from_raw(node) };
            while let Some(next) = node.next.take() {
                node.prev = None;
                node = unsafe { BoxElement::from_raw(next) };
            }
            node.prev = None;
        }
    }
}

struct LinkedListIterator<'a> {
    _list: &'a LinkedList,
    next: Option<NonNull<Element<ListNode>>>,
}

impl<'a> LinkedListIterator<'a> {
    fn new(list: &'a LinkedList) -> Self {
        let next = list.head;
        LinkedListIterator { _list: list, next }
    }
}

impl<'a> Iterator for LinkedListIterator<'a> {
    type Item = &'a Element<ListNode>;

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
    next: Option<NonNull<Element<ListNode>>>,
}

impl<'a> LinkedListIteratorMut<'a> {
    fn new(list: &'a LinkedList) -> Self {
        let next = list.head;
        LinkedListIteratorMut { _list: list, next }
    }
}

impl<'a> Iterator for LinkedListIteratorMut<'a> {
    type Item = &'a mut Element<ListNode>;

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
            let mut node = BoxElement::<ListNode>::with_capacity(1);
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
            let mut node = BoxElement::<ListNode>::with_capacity(1);
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
            let mut node = BoxElement::<ListNode>::with_capacity(1);
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
            let mut node = BoxElement::<ListNode>::with_capacity(1);
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
