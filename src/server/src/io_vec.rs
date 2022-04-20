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
    free_bufs: Option<&'static mut Buf>,
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

    fn take_buf(&mut self) -> &mut Buf {
        if let Some(buf) = self.free_bufs.take() {
            self.free_bufs = buf.next.take();
            return buf;
        }
        let ptr: &mut [u8] = self.arena.alloc_slice_fill_default(self.buf_size);
        self.arena.alloc(Buf {
            end: ptr.len(),
            rpos: 0,
            wpos: 0,
            ptr: ptr.as_mut_ptr() as _,
            next: None,
        })
    }

    #[allow(dead_code)]
    fn ret_buf(&mut self, buf: &'static mut Buf) {
        buf.next = self.free_bufs.take();
        self.free_bufs = Some(buf)
    }
}

pub struct Buf {
    pub end: usize,

    pub rpos: usize,
    wpos: usize,

    ptr: *mut u8,

    pub next: Option<&'static mut Buf>,
}

impl Buf {
    #[inline]
    fn readable_ptr(&self) -> (*mut u8, usize) {
        let len = self.wpos - self.rpos;
        unsafe { (self.ptr.add(self.rpos), len) }
    }

    #[inline]
    fn readable_slice<'a>(&'a self) -> &'a [u8] {
        let len = self.wpos - self.rpos;
        unsafe { std::slice::from_raw_parts_mut(self.ptr.add(self.rpos), len) }
    }

    #[inline]
    fn write_ptr(&self) -> (*mut u8, usize) {
        let len = self.end - self.wpos;
        unsafe { (self.ptr.add(self.wpos), len) }
    }
}

pub struct BufChain {
    pool: Rc<RefCell<Pool>>,

    pub chain: Option<&'static mut Buf>,
    wiovs: Option<Vec<IoSlice<'static>>>,
    riovs: Option<Vec<IoSliceMut<'static>>>,
    buf_cnt: usize,

    pub rbuf_pos: usize,
    pub wbuf_pos: usize,
}

impl BufChain {
    pub fn new(pool: Rc<RefCell<Pool>>, pre_init_cnt: usize) -> BufChain {
        assert!(pre_init_cnt > 0);
        let mut b = Self {
            pool,
            chain: None,
            buf_cnt: pre_init_cnt,
            rbuf_pos: 0,
            wbuf_pos: 0,
            wiovs: None,
            riovs: None,
        };
        let p = b.pool.clone();
        let mut v = (*p).borrow_mut();
        for _ in 0..b.buf_cnt {
            let nbuf = v.take_buf();
            nbuf.next = b.chain.take();
            let nbuf = nbuf as *mut Buf;
            b.chain = unsafe { Some(&mut *nbuf) };
        }
        b
    }

    pub fn as_consume_read_view<'a>(&'a self) -> impl Iterator<Item = &'a [u8]> + Clone {
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
            let s = curr_buf.as_ref().unwrap().readable_slice();
            curr_buf = &curr_buf.as_ref().unwrap().next;
            Some(s)
        })
        .take(buf_cnt)
    }

    pub fn as_io_slice(&mut self) -> (*const libc::iovec, u32, usize) {
        let buf_cnt = self.wbuf_pos - self.rbuf_pos + 1;
        let mut pos = self.rbuf_pos;
        let mut buf = &self.chain;
        while pos != 0 {
            if let Some(b) = buf {
                buf = &b.next;
                pos -= 1;
            } else {
                break;
            }
        }
        let mut curr_buf = buf.as_ref().unwrap();
        let mut total_len = 0;

        let mut iovs = self.wiovs.take().unwrap_or_default();
        iovs.clear();
        while iovs.len() < buf_cnt {
            let (iov_base, iov_len) = curr_buf.write_ptr();
            iovs.push(IoSlice::new(unsafe {
                std::slice::from_raw_parts_mut(iov_base, iov_len)
            }));
            total_len += iov_len;
            if curr_buf.next.is_some() {
                curr_buf = curr_buf.next.as_ref().unwrap();
            } else {
                break;
            }
        }
        let ret = (iovs.as_ptr().cast(), iovs.len() as _, total_len);
        self.wiovs = Some(iovs);
        ret
    }

    pub fn as_io_slice_mut(&mut self) -> (*const libc::iovec, u32, usize) {
        let buf_cnt = self.wbuf_pos - self.rbuf_pos + 1;
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
        let mut iovs = self.riovs.take().unwrap_or_default();
        iovs.clear();
        let mut tlen = 0;
        while iovs.len() < buf_cnt {
            let (iov_base, iov_len) = curr_buf.readable_ptr();
            iovs.push(IoSliceMut::new(unsafe {
                std::slice::from_raw_parts_mut(iov_base, iov_len)
            }));
            tlen += iov_len;
            if curr_buf.next.is_some() {
                curr_buf = curr_buf.next.as_mut().unwrap();
            }
        }
        let ret = (iovs.as_ptr().cast(), iovs.len() as _, tlen);
        self.riovs = Some(iovs);
        ret
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
            let mut new_pos = curr_buf.rpos + remain_size;
            if new_pos >= curr_buf.end {
                remain_size = new_pos - curr_buf.end;
                new_pos = curr_buf.end;
                curr_buf.rpos = new_pos;
                if curr_buf.next.is_none() {
                    let mut p = self.pool.borrow_mut();
                    let nbuf = p.take_buf();
                    let nbuf = nbuf as *mut Buf;
                    curr_buf.next = unsafe { Some(&mut *nbuf) };
                    self.buf_cnt += 1;
                }
                curr_buf = curr_buf.next.as_mut().unwrap();
                self.rbuf_pos += 1;
                continue;
            } else {
                curr_buf.rpos = new_pos;
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
            let mut new_pos = curr_buf.wpos + remain_size;
            if new_pos >= curr_buf.end {
                remain_size = new_pos - curr_buf.end;
                new_pos = curr_buf.end;
                curr_buf.wpos = new_pos;
                if curr_buf.next.is_none() {
                    let mut p = self.pool.borrow_mut();
                    let nbuf = p.take_buf();
                    let nbuf = nbuf as *mut Buf;
                    curr_buf.next = unsafe { Some(&mut *nbuf) };
                    self.buf_cnt += 1;
                }
                curr_buf = curr_buf.next.as_mut().unwrap();
                self.wbuf_pos += 1;
                continue;
            } else {
                curr_buf.wpos = new_pos;
                break;
            }
        }
    }

    pub fn data_remain(&self) -> usize {
        let mut remain: usize = 0;
        let mut buf = &self.chain;
        let mut pos = 0;
        while let Some(b) = buf {
            if pos >= self.rbuf_pos && pos <= self.wbuf_pos {
                let size = b.wpos - b.rpos;
                if size == 0 {
                    break;
                }
                remain += size;
            }
            buf = &b.next;
            pos += 1;
        }
        remain
    }

    pub fn put_slice(&mut self, src: &[u8]) {
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
        let mut off = 0;
        while off < src.len() {
            unsafe {
                let mut cnt;
                let dst = loop {
                    let (ptr, len) = curr_buf.write_ptr();
                    cnt = cmp::min(len, src.len() - off);
                    if cnt != 0 {
                        let dst = UninitSlice::from_raw_parts_mut(ptr, len as usize);
                        break dst;
                    }
                    if curr_buf.next.is_some() {
                        curr_buf = curr_buf.next.as_mut().unwrap();
                    } else {
                        let mut p = self.pool.borrow_mut();
                        let nbuf = p.take_buf();
                        let nbuf = nbuf as *mut Buf;
                        curr_buf.next = Some(&mut *nbuf);
                        curr_buf = curr_buf.next.as_mut().unwrap();
                        self.buf_cnt += 1;
                    }
                    self.wbuf_pos += 1;
                    continue;
                };
                ptr::copy_nonoverlapping(src[off..].as_ptr(), dst.as_mut_ptr() as *mut u8, cnt);
                off += cnt;
                curr_buf.wpos += cnt;
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
                buf.rpos = 0;
                buf.wpos = 0;
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

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use super::*;

    #[test]
    fn test_advance_pos() {
        // simulate complete read or write handle.
        let p = Rc::new(RefCell::new(Pool::new(1024 * 4)));
        let mut io_vec = BufChain::new(p, 10);
        let mut remain_read = 0;
        assert_eq!(io_vec.data_remain(), remain_read);
        io_vec.advance_wpos(10);
        remain_read += 10;
        assert_eq!(io_vec.data_remain(), remain_read);
        io_vec.advance_wpos(2 * 1024);
        remain_read += 2 * 1024;
        assert_eq!(io_vec.data_remain(), remain_read);
    }

    #[test]
    fn test_put_slice() {
        // simulate fill data into buf before send reponse.
        let p = Rc::new(RefCell::new(Pool::new(7)));
        let mut io_vec = BufChain::new(p, 3);
        io_vec.put_slice(b"a");
        assert_eq!(io_vec.data_remain(), 1);
        let v1 = io_vec.as_consume_read_view();
        assert_eq!(v1.count(), 1);
        io_vec.put_slice("b".repeat(7 + 6).as_bytes());
        let v1 = io_vec.as_consume_read_view();
        assert_eq!(v1.count(), 2);
        io_vec.put_slice("c".repeat(7).as_bytes());
        let v1 = io_vec.as_consume_read_view();
        assert_eq!(v1.count(), 3);
        io_vec.put_slice(b"d");
        let v1 = io_vec.as_consume_read_view();
        assert_eq!(v1.count(), 4);
        assert_eq!(io_vec.data_remain(), 22);

        let iovs = io_vec.as_consume_read_view().collect::<Vec<_>>();
        assert_eq!(iovs.len(), 4);
        let v = iovs[0].deref();
        assert_eq!(v, b"abbbbbb");
        let v = iovs[1].deref();
        assert_eq!(v, b"bbbbbbb");
        let v = iovs[3].deref();
        assert_eq!(v, b"d");
    }
}
