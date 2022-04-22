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

    pub(crate) fn put_at(&mut self, target: BufAddr, src: &[u8]) -> BufAddr {
        let mut buf = &mut self.nodes;
        let mut skip_nodes = target.buf;
        while skip_nodes > 0 {
            if let Some(curr) = buf {
                buf = &mut curr.next;
                skip_nodes -= 1
            } else {
                break;
            }
        }

        let mut next = target;
        let mut buf = buf.as_mut().unwrap();
        let mut idx = 0;
        while idx < src.len() {
            unsafe {
                let mut cnt;
                let dst = loop {
                    let (ptr, len) = buf.ptr(next.dat, buf.end);
                    cnt = cmp::min(len, src.len() - idx);
                    if cnt != 0 {
                        let dst = UninitSlice::from_raw_parts_mut(ptr, len as usize);
                        break dst;
                    }
                    if buf.next.is_none() {
                        let mut p = self.pool.borrow_mut();
                        let nbuf = p.take_buf();
                        let nbuf = nbuf as *mut BufNode;
                        buf.next = Some(&mut *nbuf);
                        self.node_cnt += 1;
                    }
                    buf = buf.next.as_mut().unwrap();
                    next.buf += 1;
                    next.dat = 0;
                    continue;
                };
                ptr::copy_nonoverlapping(src[idx..].as_ptr(), dst.as_mut_ptr() as *mut u8, cnt);
                idx += cnt;
                next.dat += cnt;

                if next.dat == buf.end {
                    if buf.next.is_none() {
                        let mut p = self.pool.borrow_mut();
                        let nbuf = p.take_buf();
                        let nbuf = nbuf as *mut BufNode;
                        buf.next = Some(&mut *nbuf);
                        self.node_cnt += 1;
                    }
                    next.buf += 1;
                    next.dat = 0;
                    buf = buf.next.as_mut().unwrap();
                }
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

    pub(crate) fn append_buf(&mut self) {
        let mut buf = &mut self.nodes;
        assert!(buf.is_some());
        loop {
            if let Some(curr) = buf {
                if curr.next.is_none() {
                    let mut p = self.pool.borrow_mut();
                    let nbuf = p.take_buf();
                    let nbuf = nbuf as *mut BufNode;
                    curr.next = unsafe { Some(&mut *nbuf) };
                    self.node_cnt += 1;
                    break;
                }
                buf = &mut curr.next;
            } else {
                unreachable!()
            }
        }
    }

    pub(crate) fn recycle(&mut self, n: usize) -> usize {
        let mut curr = self.nodes.take();
        let mut remain = n;
        while remain > 0 {
            let next;
            let reuse_buf;
            if let Some(buf) = curr {
                next = buf.next.take();
                buf.next = None;
                reuse_buf = buf;
            } else {
                break;
            }
            curr = next;
            let mut p = self.pool.borrow_mut();
            p.ret_buf(reuse_buf);
            remain -= 1;
        }
        self.nodes = curr;
        n - remain
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
            self.bufs.node_cnt - 1
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
                    buf = node;
                    curr_buf += 1;
                    if curr_buf > self.end_buf() {
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

    pub fn as_io_slice_mut(&mut self, iovs: &mut Vec<IoSliceMut>) -> usize {
        let buf_cnt = self.end_buf() - self.start.buf + 1;
        let bufs = self.bufs.buf_iter().skip(self.start.buf).take(buf_cnt);
        let mut tlen = 0;
        for (i, node) in bufs.enumerate() {
            let start = if i == 0 { self.start.dat } else { node.begin };
            let end = if i == buf_cnt - 1 {
                if let Some(end) = self.end {
                    end.dat
                } else {
                    node.end
                }
            } else {
                node.end
            };
            let (iov_base, iov_len) = node.ptr(start, end);
            iovs.push(IoSliceMut::new(unsafe {
                std::slice::from_raw_parts_mut(iov_base, iov_len)
            }));
            tlen += iov_len;
            assert!(iov_len > 0);
        }
        assert!(!iovs.is_empty());
        tlen
    }

    pub fn as_io_slice(&mut self, iovs: &mut Vec<IoSlice>) -> usize {
        let buf_cnt = self.end_buf() - self.start.buf + 1;
        let bufs = self.bufs.buf_iter().skip(self.start.buf).take(buf_cnt);
        let mut tlen = 0;
        for (i, node) in bufs.enumerate() {
            let start = if i == 0 { self.start.dat } else { node.begin };
            let end = if i == buf_cnt - 1 {
                if let Some(end) = self.end {
                    end.dat
                } else {
                    node.end
                }
            } else {
                node.end
            };
            let (iov_base, iov_len) = node.ptr(start, end);
            iovs.push(IoSlice::new(unsafe {
                std::slice::from_raw_parts_mut(iov_base, iov_len)
            }));
            tlen += iov_len;
        }
        tlen
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
        let mut ret = Vec::with_capacity(self.len());
        for (i, buf) in v.enumerate() {
            let start = if i == 0 { self.start.dat } else { buf.begin };
            let end = if i == buf_cnt - 1 {
                if let Some(end) = self.end {
                    end.dat
                } else {
                    buf.end
                }
            } else {
                buf.end
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
                if let Some(end) = self.end {
                    end.dat
                } else {
                    node.end
                }
            } else {
                node.end
            };
            total += end - start;
        }
        total
    }
}
