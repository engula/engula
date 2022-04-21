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

    // fill -> inf+
    #[inline]
    fn slice(&self, start: usize, end: usize) -> &'_ [u8] {
        let len = end - start;
        unsafe { std::slice::from_raw_parts_mut(self.ptr.add(start), len) }
    }

    #[inline]
    fn ptr_mut(&self, start: usize, end: usize) -> (*mut u8, usize) {
        let len = end - start;
        unsafe { (self.ptr.add(start), len) }
    }
}

pub struct Bufs {
    pool: Rc<RefCell<Pool>>,

    pub nodes: Option<&'static mut BufNode>,
    pub node_cnt: usize,
}

#[derive(Default, PartialEq, Clone)]
pub struct BufAddr {
    buf: usize, // NB: it's related to original bufs's idx.
    dat: usize,
}

impl Bufs {
    pub(crate) fn new(pool: Rc<RefCell<Pool>>, init_nodes: usize) -> Self {
        assert!(init_nodes > 0);
        let bufs = Self {
            pool,
            nodes: None,
            node_cnt: init_nodes,
        };
        let mut p = bufs.pool.clone().borrow_mut();
        for _ in 0..bufs.node_cnt {
            let node = p.take_buf();
            node.next = bufs.nodes.take();
            let nbuf = node as *mut BufNode;
            bufs.nodes = unsafe { Some(&mut *node) };
        }
        bufs
    }

    pub(crate) fn put_after(&self, filled: BufAddr, src: &[u8]) -> BufAddr {
        let new_filled = filled.clone();
        let buf = self.buf_iter().skip(filled.buf).next().unwrap();
        let mut flushed = 0;
        while flushed < src.len() {
            unsafe {
                let mut cnt;
                let dst = loop {
                    let start = if new_filled.buf == filled.buf {
                        filled.dat
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
                        buf = buf.next.as_mut().unwrap();
                    } else {
                        let mut p = self.pool.borrow_mut();
                        let nbuf = p.take_buf();
                        let nbuf = nbuf as *mut BufNode;
                        buf.next = Some(&mut *nbuf);
                        buf = buf.next.as_mut().unwrap();
                        self.node_cnt += 1;
                    }
                    new_filled.buf += 1;
                    continue;
                };
                ptr::copy_nonoverlapping(src[flushed..].as_ptr(), dst.as_mut_ptr() as *mut u8, cnt);
                flushed += cnt;
                new_filled.dat += cnt;
            }
        }
        new_filled
    }

    pub fn slice(&self, start: BufAddr, end: BufAddr) -> BufSlice {
        BufSlice {
            bufs: self,
            start,
            end,
        }
    }

    fn buf_iter(&self) -> impl Iterator<Item = &&mut BufNode> {
        let mut curr = &self.nodes;
        std::iter::from_fn(move || {
            if let Some(node) = curr {
                let ret = curr;
                curr = &node.next;
                Some(node)
            } else {
                None
            }
        })
    }
}

pub struct BufSlice<'a> {
    bufs: &'a Bufs,

    start: BufAddr,
    end: BufAddr,
}

impl<'a> BufSlice<'a> {
    pub fn advance_addr(&self, origin: BufAddr, n: usize) -> Option<BufAddr> {
        let buf_cnt = self.end.buf - self.start.buf + 1;
        let mut buf = self.bufs.buf_iter().nth(origin.buf).unwrap();
        let mut curr_buf = origin.buf;
        let mut curr_pos = origin.dat;
        loop {
            if curr_pos + n >= buf.end {
                if let Some(node) = buf.next {
                    n -= buf.end - curr_pos;
                    buf = &node;
                    curr_buf += 1;
                    curr_pos = 0;
                } else {
                    return None;
                }
                continue;
            }
            return Some(BufAddr {
                buf: curr_buf,
                dat: curr_pos,
            });
        }
    }

    pub fn as_io_slice_mut(&mut self, iovs: &mut Vec<IoSliceMut>) {
        let buf_cnt = self.end.buf - self.start.buf + 1;
        let bufs = self.bufs.buf_iter().skip(self.start.buf).take(buf_cnt);
        for (i, node) in bufs.enumerate() {
            let start = if i == 0 { self.start.dat } else { node.begin };
            let end = if i == buf_cnt - 1 {
                self.end.dat
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
        let buf_cnt = self.end.buf - self.start.buf + 1;
        let bufs = self.bufs.buf_iter().skip(self.start.buf).take(buf_cnt);
        for (i, node) in bufs.enumerate() {
            let start = if i == 0 { self.start.dat } else { node.begin };
            let end = if i == buf_cnt - 1 {
                self.end.dat
            } else {
                node.end
            };
            let (iov_base, iov_len) = node.ptr(start, end);
            iovs.push(IoSlice::new(unsafe {
                std::slice::from_raw_parts_mut(iov_base, iov_len)
            }));
        }
    }
}

pub struct IoVec {
    pub riovs: Option<Vec<IoSliceMut<'static>>>,
    pub wiovs: Option<Vec<IoSlice<'static>>>,
}

impl IoVec {
    pub fn as_consume_read_view(&self) -> impl Iterator<Item = &'_ [u8]> + Clone {
        let buf_cnt = self.wbuf_pos - self.rbuf_pos + 1;
        let mut buf = &self.chain;
        let mut pos = self.rbuf_pos;
        while pos != 0 {
            if let Some(b) = buf {
                buf = &b.next;
                pos -= 1;
            } else {
                break;
            }
        }

        let mut curr_buf = buf;
        std::iter::from_fn(move || {
            if curr_buf.is_none() {
                return None;
            }
            let s = curr_buf.as_ref().unwrap().slice();
            curr_buf = &curr_buf.as_ref().unwrap().next;
            Some(s)
        })
        .take(buf_cnt)
    }

    pub fn advance_rpos(&mut self, size: usize) {
        let mut pos = self.rbuf_pos;
        let mut buf = &mut self.chain;
        while pos != 0 {
            if let Some(b) = buf {
                buf = &mut b.next;
                pos -= 1;
            } else {
                break;
            }
        }
        let mut curr_buf = buf.as_mut().unwrap();
        let mut remain_size = size;
        loop {
            let mut new_pos = curr_buf.begin + remain_size;
            if new_pos >= curr_buf.end {
                remain_size = new_pos - curr_buf.end;
                new_pos = curr_buf.end;
                curr_buf.begin = new_pos;
                if curr_buf.next.is_none() {
                    let mut p = self.pool.borrow_mut();
                    let nbuf = p.take_buf();
                    let nbuf = nbuf as *mut BufNode;
                    curr_buf.next = unsafe { Some(&mut *nbuf) };
                    self.buf_cnt += 1;
                }
                curr_buf = curr_buf.next.as_mut().unwrap();
                self.rbuf_pos += 1;
                continue;
            } else {
                curr_buf.begin = new_pos;
                break;
            }
        }
    }

    pub fn advance_wpos(&mut self, size: usize) {
        let mut pos = self.wbuf_pos;
        let mut buf = &mut self.chain;
        while pos != 0 {
            if let Some(b) = buf {
                buf = &mut b.next;
                pos -= 1;
            } else {
                break;
            }
        }
        let mut curr_buf = buf.as_mut().unwrap();
        let mut remain_size = size;
        loop {
            let mut new_pos = curr_buf.filled + remain_size;
            if new_pos >= curr_buf.end {
                remain_size = new_pos - curr_buf.end;
                new_pos = curr_buf.end;
                curr_buf.filled = new_pos;
                if curr_buf.next.is_none() {
                    let mut p = self.pool.borrow_mut();
                    let nbuf = p.take_buf();
                    let nbuf = nbuf as *mut BufNode;
                    curr_buf.next = unsafe { Some(&mut *nbuf) };
                    self.buf_cnt += 1;
                }
                curr_buf = curr_buf.next.as_mut().unwrap();
                self.wbuf_pos += 1;
                continue;
            } else {
                curr_buf.filled = new_pos;
                break;
            }
        }
    }

    pub(crate) fn recycle(&mut self) {
        if self.rbuf_pos == 0 {
            return;
        }
        let wait_clean = self.rbuf_pos;
        let mut clean_idx = 0;
        let mut curr = self.chain.take();
        while clean_idx < wait_clean {
            let next;
            let reuse_buf;
            if let Some(buf) = curr {
                next = buf.next.take();
                buf.next = None;
                buf.begin = 0;
                buf.filled = 0;
                reuse_buf = buf;
            } else {
                break;
            }
            curr = next;
            let mut p = self.pool.borrow_mut();
            p.ret_buf(reuse_buf);
            clean_idx += 1;
        }
        self.rbuf_pos = 0;
        self.wbuf_pos = if self.wbuf_pos > wait_clean {
            self.wbuf_pos - wait_clean
        } else {
            0
        };
        self.chain = curr;
    }
}
