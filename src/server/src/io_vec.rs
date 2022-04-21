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

use std::{
    cell::RefCell,
    cmp,
    io::{IoSlice, IoSliceMut},
    ptr,
    rc::Rc,
};

use bytes::buf::UninitSlice;

#[derive(Debug)]
pub struct Pool {
    arena: bumpalo::Bump,
    free_bufs: Option<&'static mut BufNode>,
    buf_size: usize,
}

impl Pool {
    pub fn new(buf_size: usize) -> Self {
        Self {
            arena: bumpalo::Bump::new(),
            free_bufs: None,
            buf_size,
        }
    }

    fn take_buf(&mut self) -> &mut BufNode {
        if let Some(buf) = self.free_bufs.take() {
            self.free_bufs = buf.next.take();
            return buf;
        }
        let ptr: &mut [u8] = self.arena.alloc_slice_fill_default(self.buf_size);
        self.arena.alloc(BufNode {
            end: ptr.len(),
            begin: 0,
            ptr: ptr.as_mut_ptr() as _,
            next: None,
        })
    }

    #[allow(dead_code)]
    fn ret_buf(&mut self, buf: &'static mut BufNode) {
        buf.next = self.free_bufs.take();
        self.free_bufs = Some(buf)
    }
}

#[derive(Debug)]
pub struct BufNode {
    ptr: *mut u8,

    pub begin: usize,
    pub end: usize,

    pub next: Option<&'static mut BufNode>,
}

impl BufNode {
    #[inline]
    fn ptr(&self, start: usize, end: usize) -> (*mut u8, usize) {
        let len = end - start;
        unsafe { (self.ptr.add(start), len) }
    }

    #[inline]
    pub fn slice(&self, start: usize, end: usize) -> &'_ [u8] {
        let len = end - start;
        unsafe { std::slice::from_raw_parts_mut(self.ptr.add(start), len) }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.end - self.begin
    }
}

#[derive(Debug)]
pub struct Bufs {
    pool: Rc<RefCell<Pool>>,

    pub nodes: Option<&'static mut BufNode>,
    pub node_cnt: usize,
}

#[derive(PartialEq, Clone, Debug, Copy)]
pub struct BufAddr {
    pub buf: usize, // NB: it's related to original bufs's idx.
    pub dat: usize,
}

impl BufAddr {
    pub fn zero() -> Self {
        Self { buf: 0, dat: 0 }
    }
}

impl Bufs {
    pub(crate) fn new(pool: Rc<RefCell<Pool>>, init_nodes: usize) -> Self {
        assert!(init_nodes > 0);
        let mut b = Self {
            pool,
            nodes: None,
            node_cnt: init_nodes,
        };
        let p = b.pool.clone();
        let mut v = (*p).borrow_mut();
        for _ in 0..b.node_cnt {
            let nbuf = v.take_buf();
            nbuf.next = b.nodes.take();
            let nbuf = nbuf as *mut BufNode;
            b.nodes = unsafe { Some(&mut *nbuf) };
        }
        b
    }

    pub(crate) fn put_at(&self, target: BufAddr, src: &[u8]) -> BufAddr {
        let mut next = target.clone();
        let mut buf = self.buf_iter().skip(target.buf).next().unwrap();
        let mut flushed = 0;
        while flushed < src.len() {
            unsafe {
                let mut cnt;
                let dst = loop {
                    let start = if next.buf == target.buf {
                        target.dat
                    } else {
                        0
                    };
                    let (ptr, len) = buf.ptr(start, buf.end);
                    cnt = cmp::min(len, src.len() - flushed);
                    if cnt != 0 {
                        let dst = UninitSlice::from_raw_parts_mut(ptr, len as usize);
                        break dst;
                    }
                    if buf.next.is_some() {
                        buf = buf.next.as_ref().unwrap();
                    } else {
                        // let mut p = self.pool.borrow_mut();
                        // let nbuf = p.take_buf();
                        // let nbuf = nbuf as *mut Buf;
                        // curr_buf.next = Some(&mut *nbuf);
                        // curr_buf = curr_buf.next.as_mut().unwrap();
                        // self.buf_cnt += 1;
                        todo!("lifecyle question")
                    }
                    next.buf += 1;
                    continue;
                };
                ptr::copy_nonoverlapping(src[flushed..].as_ptr(), dst.as_mut_ptr() as *mut u8, cnt);
                flushed += cnt;
                next.dat += cnt;
            }
        }
        next
    }

    pub fn slice(&self, start: BufAddr, end: Option<BufAddr>) -> BufSlice {
        BufSlice {
            bufs: self,
            start,
            end,
        }
    }

    pub fn buf_iter(&self) -> impl Iterator<Item = &&mut BufNode> {
        let mut curr = &self.nodes;
        std::iter::from_fn(move || {
            if let Some(node) = curr {
                curr = &node.next;
                Some(node)
            } else {
                None
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct BufSlice<'a> {
    bufs: &'a Bufs,

    start: BufAddr,
    end: Option<BufAddr>,
}

impl<'a> BufSlice<'a> {
    fn end_buf(&self) -> usize {
        if let Some(add) = self.end {
            add.buf
        } else {
            self.bufs.node_cnt
        }
    }

    pub fn advance_addr(&self, origin: BufAddr, mut n: usize) -> Option<BufAddr> {
        let mut buf = self.bufs.buf_iter().nth(origin.buf).unwrap();
        let mut curr_buf = origin.buf;
        let mut curr_pos = origin.dat;
        loop {
            if curr_pos + n >= buf.end {
                if let Some(node) = &buf.next {
                    n -= buf.end - curr_pos;
                    buf = &node;
                    curr_buf += 1;
                    if curr_buf == self.end_buf() {
                        return None;
                    }
                    curr_pos = 0;
                } else {
                    return None;
                }
                continue;
            }
            return Some(BufAddr {
                buf: curr_buf,
                dat: curr_pos + n,
            });
        }
    }

    pub fn as_io_slice_mut(&mut self, iovs: &mut Vec<IoSliceMut>) {
        let buf_cnt = self.end_buf() - self.start.buf + 1;
        let bufs = self.bufs.buf_iter().skip(self.start.buf).take(buf_cnt);
        for (i, node) in bufs.enumerate() {
            let start = if i == 0 { self.start.dat } else { node.begin };
            let end = if i == buf_cnt - 1 {
                self.end.unwrap().dat
            } else {
                node.end
            };
            let (iov_base, iov_len) = node.ptr(start, end);
            iovs.push(IoSliceMut::new(unsafe {
                std::slice::from_raw_parts_mut(iov_base, iov_len)
            }));
        }
    }

    pub fn as_io_slice(&mut self, iovs: &mut Vec<IoSlice>) {
        let buf_cnt = self.end_buf() - self.start.buf + 1;
        let bufs = self.bufs.buf_iter().skip(self.start.buf).take(buf_cnt);
        for (i, node) in bufs.enumerate() {
            let start = if i == 0 { self.start.dat } else { node.begin };
            let end = if i == buf_cnt - 1 {
                self.end.unwrap().dat
            } else {
                node.end
            };
            let (iov_base, iov_len) = node.ptr(start, end);
            iovs.push(IoSlice::new(unsafe {
                std::slice::from_raw_parts_mut(iov_base, iov_len)
            }));
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        if self.end.is_none() {
            return false;
        }
        self.start == self.end.unwrap()
    }

    pub(crate) fn as_vec(&self) -> Vec<u8> {
        // TODO: temp method to test.
        let buf_cnt = self.end_buf() - self.start.buf + 1;
        let v = self.bufs.buf_iter().skip(self.start.buf).take(buf_cnt);
        let mut ret = Vec::new();
        for (i, buf) in v.enumerate() {
            let start = if i == self.start.buf {
                self.start.dat
            } else {
                0
            };
            let end = if i == self.end_buf() {
                self.end.unwrap().dat
            } else {
                0
            };
            let buf = buf.slice(start, end);
            ret.extend_from_slice(buf);
        }
        ret
    }

    pub(crate) fn len(&self) -> usize {
        let mut total = 0;
        let buf_cnt = self.end_buf() - self.start.buf + 1;
        let bufs = self.bufs.buf_iter().skip(self.start.buf).take(buf_cnt);
        for (i, node) in bufs.enumerate() {
            let start = if i == 0 { self.start.dat } else { node.begin };
            let end = if i == buf_cnt - 1 {
                self.end.unwrap().dat
            } else {
                node.end
            };
            total += end - start;
        }
        total
    }
}
