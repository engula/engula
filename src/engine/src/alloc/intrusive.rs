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

use std::{marker::PhantomData, ptr::NonNull};

pub struct LinkedList<T, A: ListNodeAdaptor<T>> {
    head: Option<NonNull<T>>,
    tail: Option<NonNull<T>>,
    len: usize,
    marker: PhantomData<(Box<T>, A)>,
}

#[repr(C)]
pub struct ListNode<T> {
    next: Option<NonNull<T>>,
    prev: Option<NonNull<T>>,
    marker: PhantomData<T>,
}

pub trait ListNodeAdaptor<T>: Default {
    fn node_mut(data: &mut T) -> &mut ListNode<T>;
    fn node(data: &T) -> &ListNode<T>;
    unsafe fn node_offset(data: NonNull<T>) -> NonNull<ListNode<T>>;

    #[inline]
    fn take_next(data: &mut T) -> Option<NonNull<T>> {
        Self::node_mut(data).next
    }

    #[inline]
    fn take_prev(data: &mut T) -> Option<NonNull<T>> {
        Self::node_mut(data).next
    }

    #[inline]
    fn next(data: &T) -> Option<NonNull<T>> {
        Self::node(data).next
    }

    #[inline]
    fn prev(data: &T) -> Option<NonNull<T>> {
        Self::node(data).prev
    }

    #[inline]
    unsafe fn set_next_ptr(data: NonNull<T>, next: Option<NonNull<T>>) {
        Self::node_offset(data).as_mut().next = next;
    }

    #[inline]
    unsafe fn set_prev_ptr(data: NonNull<T>, prev: Option<NonNull<T>>) {
        Self::node_offset(data).as_mut().prev = prev;
    }

    #[inline]
    fn set_next(data: &mut T, next: Option<NonNull<T>>) {
        unsafe { Self::set_next_ptr(NonNull::from(data), next) };
    }

    #[inline]
    fn set_prev(data: &mut T, prev: Option<NonNull<T>>) {
        unsafe { Self::set_prev_ptr(NonNull::from(data), prev) };
    }
}

pub struct Iter<'a, T: 'a, A: ListNodeAdaptor<T>> {
    head: Option<NonNull<T>>,
    tail: Option<NonNull<T>>,
    len: usize,
    marker: PhantomData<(&'a T, A)>,
}

impl<T> ListNode<T> {
    pub fn new() -> Self {
        ListNode {
            prev: None,
            next: None,
            marker: PhantomData,
        }
    }
}

impl<T, A: ListNodeAdaptor<T>> LinkedList<T, A> {
    /// Adds the given node to the front of the list.
    #[inline]
    fn push_front_node(&mut self, mut node: Box<T>) {
        // This method takes care not to create mutable references to whole nodes,
        // to maintain validity of aliasing pointers into `element`.
        unsafe {
            A::set_next(&mut node, self.head);
            A::set_prev(&mut node, None);
            let node = Some(Box::leak(node).into());

            match self.head {
                None => self.tail = node,
                // Not creating new mutable (unique!) references overlapping `element`.
                Some(head) => A::set_prev_ptr(head, node),
            }

            self.head = node;
            self.len += 1;
        }
    }

    /// Removes and returns the node at the front of the list.
    #[inline]
    fn pop_front_node(&mut self) -> Option<Box<T>> {
        // This method takes care not to create mutable references to whole nodes,
        // to maintain validity of aliasing pointers into `element`.
        self.head.map(|node| unsafe {
            let mut node = Box::from_raw(node.as_ptr());
            self.head = A::take_next(&mut node);

            match self.head {
                None => self.tail = None,
                // Not creating new mutable (unique!) references overlapping `element`.
                Some(head) => A::set_prev_ptr(head, None),
            }

            self.len -= 1;
            node
        })
    }

    /// Adds the given node to the back of the list.
    #[inline]
    fn push_back_node(&mut self, mut node: Box<T>) {
        // This method takes care not to create mutable references to whole nodes,
        // to maintain validity of aliasing pointers into `element`.
        unsafe {
            A::set_next(&mut node, None);
            A::set_prev(&mut node, self.tail);
            let node = Some(Box::leak(node).into());

            match self.tail {
                None => self.head = node,
                // Not creating new mutable (unique!) references overlapping `element`.
                Some(tail) => A::set_next_ptr(tail, node),
            }

            self.tail = node;
            self.len += 1;
        }
    }

    /// Removes and returns the node at the back of the list.
    #[inline]
    fn pop_back_node(&mut self) -> Option<Box<T>> {
        // This method takes care not to create mutable references to whole nodes,
        // to maintain validity of aliasing pointers into `element`.
        self.tail.map(|node| unsafe {
            let mut node = Box::from_raw(node.as_ptr());
            self.tail = A::take_prev(&mut node);

            match self.tail {
                None => self.head = None,
                // Not creating new mutable (unique!) references overlapping `element`.
                Some(tail) => A::set_next_ptr(tail, None),
            }

            self.len -= 1;
            node
        })
    }
}

#[allow(dead_code)]
impl<T, A: ListNodeAdaptor<T>> LinkedList<T, A> {
    pub fn new() -> Self {
        LinkedList {
            head: None,
            tail: None,
            len: 0,
            marker: PhantomData,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn front(&self) -> Option<&T> {
        unsafe { self.head.as_ref().map(|e| e.as_ref()) }
    }

    #[inline]
    pub fn front_mut(&mut self) -> Option<&mut T> {
        unsafe { self.head.as_mut().map(|e| e.as_mut()) }
    }

    #[inline]
    pub fn back(&self) -> Option<&T> {
        unsafe { self.tail.as_ref().map(|e| e.as_ref()) }
    }

    #[inline]
    pub fn back_mut(&mut self) -> Option<&mut T> {
        unsafe { self.tail.as_mut().map(|e| e.as_mut()) }
    }

    #[inline]
    pub fn push_front(&mut self, elt: Box<T>) {
        self.push_front_node(elt);
    }

    #[inline]
    pub fn pop_front(&mut self) -> Option<Box<T>> {
        self.pop_front_node()
    }

    #[inline]
    pub fn push_back(&mut self, elt: Box<T>) {
        self.push_back_node(elt);
    }

    pub fn pop_back(&mut self) -> Option<Box<T>> {
        self.pop_back_node()
    }

    /// Unlinks the specified node from the current list.
    ///
    /// Warning: this will not check that the provided node belongs to the current list.
    ///
    /// This method takes care not to create mutable references to `element`, to
    /// maintain validity of aliasing pointers.
    #[inline]
    pub unsafe fn unlink_node(&mut self, node: NonNull<T>) -> Box<T> {
        let mut node = Box::from_raw(node.as_ptr());

        // this one is ours now, we can create an &mut.
        let next = A::take_next(&mut node);
        let prev = A::take_prev(&mut node);

        // Not creating new mutable (unique!) references overlapping `element`.
        match prev {
            Some(prev) => A::set_next_ptr(prev, next),
            // this node is the head node
            None => self.head = next,
        };

        match next {
            Some(next) => A::set_prev_ptr(next, prev),
            // this node is the tail node
            None => self.tail = prev,
        };

        self.len -= 1;
        node
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, T, A> {
        Iter {
            head: self.head,
            tail: self.tail,
            len: self.len,
            marker: PhantomData,
        }
    }
}

impl<T, A: ListNodeAdaptor<T>> Drop for LinkedList<T, A> {
    fn drop(&mut self) {
        while self.pop_front_node().is_some() {}
    }
}

impl<'a, T, A: ListNodeAdaptor<T>> Iterator for Iter<'a, T, A> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<&'a T> {
        if self.len == 0 {
            None
        } else {
            self.head.map(|node| unsafe {
                // Need an unbound lifetime to get 'a
                let node = &*node.as_ptr();
                self.len -= 1;
                self.head = A::next(node);
                node
            })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }

    #[inline]
    fn last(mut self) -> Option<&'a T> {
        self.next_back()
    }
}

impl<'a, T, A: ListNodeAdaptor<T>> DoubleEndedIterator for Iter<'a, T, A> {
    #[inline]
    fn next_back(&mut self) -> Option<&'a T> {
        if self.len == 0 {
            None
        } else {
            self.tail.map(|node| unsafe {
                // Need an unbound lifetime to get 'a
                let node = &*node.as_ptr();
                self.len -= 1;
                self.tail = A::prev(node);
                node
            })
        }
    }
}

#[macro_export]
macro_rules! intrusive_linked_list_adaptor {
    ($name:ident, $node_type: ty, $field_name: ident) => {
        #[derive(Default)]
        struct $name;

        impl ListNodeAdaptor<$node_type> for $name {
            fn node_mut(data: &mut $node_type) -> &mut ListNode<$node_type> {
                &mut data.$field_name
            }

            fn node(data: &$node_type) -> &ListNode<$node_type> {
                &data.$field_name
            }

            #[allow(clippy::missing_safety_doc)]
            unsafe fn node_offset(data: NonNull<$node_type>) -> NonNull<ListNode<$node_type>> {
                NonNull::new_unchecked(std::ptr::addr_of!(data.as_ref().$field_name) as *mut _)
            }
        }
    };
}
