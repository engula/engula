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

use std::{iter::FusedIterator, marker::PhantomData, ptr::NonNull};

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

/// An iterator over the elements of a `LinkedList`.
///
/// This `struct` is created by [`LinkedList::iter()`]. See its
/// documentation for more.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct Iter<'a> {
    head: Option<NonNull<Element<ListNode>>>,
    tail: Option<NonNull<Element<ListNode>>>,
    len: usize,
    marker: PhantomData<&'a Element<ListNode>>,
}

/// An iterator over the elements of a `LinkedList`.
///
/// This `struct` is created by [`LinkedList::iter_mut()`]. See its
/// documentation for more.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IterMut<'a> {
    head: Option<NonNull<Element<ListNode>>>,
    tail: Option<NonNull<Element<ListNode>>>,
    len: usize,
    marker: PhantomData<&'a mut Element<ListNode>>,
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
        Iter {
            head: self.head,
            tail: self.tail,
            len: self.len as usize,
            marker: PhantomData,
        }
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Element<ListNode>> {
        IterMut {
            head: self.head,
            tail: self.tail,
            len: self.len as usize,
            marker: PhantomData,
        }
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
        ObjectType::LINKED_LIST.bits()
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

impl<'a> Iterator for Iter<'a> {
    type Item = &'a Element<ListNode>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            None
        } else {
            self.head.map(|node| unsafe {
                let node = node.as_ref();
                self.len -= 1;
                self.head = node.next;
                node
            })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }

    #[inline]
    fn last(mut self) -> Option<Self::Item> {
        self.next_back()
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            None
        } else {
            self.tail.map(|node| unsafe {
                let node = node.as_ref();
                self.len -= 1;
                self.tail = node.prev;
                node
            })
        }
    }
}

impl ExactSizeIterator for Iter<'_> {}

impl FusedIterator for Iter<'_> {}

impl<'a> Iterator for IterMut<'a> {
    type Item = &'a mut Element<ListNode>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            None
        } else {
            self.head.map(|mut node| unsafe {
                let node = node.as_mut();
                self.len -= 1;
                self.head = node.next;
                node
            })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }

    #[inline]
    fn last(mut self) -> Option<Self::Item> {
        self.next_back()
    }
}

impl<'a> DoubleEndedIterator for IterMut<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            None
        } else {
            self.tail.map(|mut node| unsafe {
                let node = node.as_mut();
                self.len -= 1;
                self.tail = node.prev;
                node
            })
        }
    }
}

impl ExactSizeIterator for IterMut<'_> {}

impl FusedIterator for IterMut<'_> {}

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
