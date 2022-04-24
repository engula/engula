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

use std::io::{IoSlice, IoSliceMut};

use crate::{
    io_vec::{self, BufAddr, BufSlice},
    Frame, FrameError,
};

pub struct Cursor<'b> {
    pub bufs: &'b mut io_vec::Bufs,

    pos: io_vec::BufAddr,
    min_readable: io_vec::BufAddr,
    max_readable: io_vec::BufAddr,
    offset: usize,
}

impl<'b> Cursor<'b> {
    pub fn new(
        bufs: &'b mut io_vec::Bufs,
        min_readable: io_vec::BufAddr,
        max_readable: io_vec::BufAddr,
    ) -> Self {
        Self {
            bufs,
            pos: min_readable,
            offset: 0,
            min_readable,
            max_readable,
        }
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn reset(&mut self) {
        self.pos = self.min_readable;
        self.offset = 0;
    }

    pub fn get_u8(&mut self) -> u8 {
        let node = self.bufs.buf_iter().nth(self.pos.buf).unwrap();
        let buf = node.slice(self.pos.dat, node.end);
        let res = buf[0];

        // TODO: extract this.
        self.pos.dat += 1;
        self.offset += 1;
        if self.pos.dat == node.end {
            self.pos.dat = 0;
            self.pos.buf += 1;
        }
        res
    }

    pub fn peek_u8(&mut self) -> u8 {
        let buf = self.bufs.buf_iter().nth(self.pos.buf).unwrap();
        let buf = buf.slice(self.pos.dat, buf.end);
        buf[0]
    }

    pub(crate) fn get_line(&mut self) -> BufSlice<'_> {
        const WAIT_R: u8 = 1;
        const WAIT_N: u8 = 2;

        let start = self.pos;
        let buf_cnt = self.max_readable.buf - self.min_readable.buf + 1;
        let iovs = self.bufs.buf_iter().skip(self.pos.buf).take(buf_cnt);

        let mut buf_idx = self.pos.buf;
        let mut data_idx = self.pos.dat;
        let mut new_offset = self.offset;

        let mut get_line_state = WAIT_R;
        'next_buf: for buf in iovs {
            let buf = buf.slice(buf.begin, buf.end); // handle on origin's buf idx, so get full buf.
            if buf.is_empty() {
                break 'next_buf;
            }
            'next_byte: for i in data_idx..buf.len() {
                match get_line_state {
                    WAIT_R => {
                        if buf[i] == b'\r' {
                            get_line_state = WAIT_N;
                            continue 'next_byte;
                        }
                    }
                    WAIT_N => {
                        if buf[i] != b'\n' {
                            get_line_state = WAIT_R;
                            continue 'next_byte;
                        }
                        new_offset += i + 1 - data_idx;
                        self.pos = if i + 1 == buf.len() {
                            BufAddr {
                                buf: buf_idx + 1,
                                dat: 0,
                            }
                        } else {
                            BufAddr {
                                buf: buf_idx,
                                dat: i + 1,
                            }
                        };
                        self.offset = new_offset;
                        break 'next_buf;
                    }
                    _ => unreachable!(),
                }
            }
            new_offset += buf.len() - data_idx;
            data_idx = 0;
            buf_idx += 1;
        }

        self.bufs.slice(start, Some(self.pos))
    }

    pub(crate) fn advance(&mut self, n: usize) {
        let BufAddr {
            buf: mut buf_idx,
            dat: mut dat_idx,
        } = self.pos;

        let buf_cnt = self.max_readable.buf - self.min_readable.buf + 1;
        let mut iter = self.bufs.buf_iter().skip(buf_idx).take(buf_cnt);
        let mut remain_n = n;
        while remain_n > 0 {
            let buf = iter.next().unwrap();
            if dat_idx + remain_n < buf.len() {
                dat_idx += remain_n;
                break;
            }
            buf_idx += 1;
            remain_n -= buf.len() - dat_idx;
            dat_idx = 0;
        }
        self.pos = BufAddr {
            buf: buf_idx,
            dat: dat_idx,
        };
        self.offset += n;
    }

    pub(crate) fn peek_n(&self, n: usize) -> BufSlice {
        let BufAddr {
            buf: mut buf_idx,
            dat: mut dat_idx,
        } = self.pos;

        let buf_cnt = self.max_readable.buf - self.min_readable.buf + 1;
        let mut iovs = self.bufs.buf_iter().skip(buf_idx).take(buf_cnt);

        let start = self.pos;
        let end;
        let mut remain_n = n;
        loop {
            let buf = iovs.next().unwrap();
            if dat_idx + remain_n > buf.len() {
                remain_n = remain_n + dat_idx - buf.len();
                buf_idx += 1;
                dat_idx = 0;
            } else {
                end = BufAddr {
                    buf: buf_idx,
                    dat: dat_idx + remain_n,
                };
                break;
            }
        }

        self.bufs.slice(start, Some(end))
    }

    pub fn data_remain(&self, n: usize) -> bool {
        let mut remain = (self.offset + n) as i64;
        let buf_cnt = self.max_readable.buf - self.min_readable.buf + 1;
        let bufs = self
            .bufs
            .buf_iter()
            .skip(self.min_readable.buf)
            .take(buf_cnt);
        for (i, node) in bufs.enumerate() {
            let start = if i == 0 {
                self.min_readable.dat
            } else {
                node.begin
            };
            let end = if i == buf_cnt - 1 {
                self.max_readable.dat
            } else {
                node.end
            };
            remain -= (end - start) as i64;
            if remain <= 0 {
                return true;
            }
        }
        false
    }
}

pub struct ReadBuf {
    pub bufs: io_vec::Bufs,
    pub min_readable: io_vec::BufAddr,
    pub max_readable: io_vec::BufAddr,

    pub riovs: Option<Vec<IoSliceMut<'static>>>,

    pub max_idle_buf_cnt: usize,
}

impl ReadBuf {
    pub fn new(bufs: io_vec::Bufs, max_idle_buf_cnt: usize) -> Self {
        Self {
            bufs,
            min_readable: io_vec::BufAddr::zero(),
            max_readable: io_vec::BufAddr::zero(),
            riovs: None,
            max_idle_buf_cnt,
        }
    }

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    pub fn parse_frame(&mut self) -> Option<Frame> {
        use FrameError::Incomplete;

        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&mut self.bufs, self.min_readable, self.max_readable);

        // The first step is to check if enough data has been buffered to parse
        // a single frame. This step is usually much faster than doing a full
        // parse of the frame, and allows us to skip allocating data structures
        // to hold the frame data unless we know the full frame has been
        // received.
        match Frame::check(&mut buf) {
            Ok(_) => {
                // The `check` function will have advanced the cursor until the
                // end of the frame. Since the cursor had position set to zero
                // before `Frame::check` was called, we obtain the length of the
                // frame by checking the cursor position.
                let len = buf.offset();

                // Reset the cursor to the beginning of the buf.
                buf.reset();

                // Parse the frame from the buffer. This allocates the necessary
                // structures to represent the frame and returns the frame
                // value.
                //
                // If the encoded frame representation is invalid, an error is
                // returned. This should terminate the **current** connection
                // but should not impact any other connected client.
                let frame = Frame::parse(&mut buf).unwrap();

                // Discard the parsed data from the read buffer.
                //
                // When `advance` is called on the read buffer, all of the data
                // up to `len` is discarded. The details of how this works is
                // left to `BytesMut`. This is often done by moving an internal
                // cursor, but it may be done by reallocating and copying data.
                self.advance_min_readable(len);

                // Return the parsed frame to the caller.
                Some(frame)
            }
            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(Incomplete) => None,
            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(err) => panic!("{:?}", err),
        }
    }

    pub(crate) fn wait_fill_io_slice_mut(&mut self) -> (*const libc::iovec, u32, usize) {
        let mut wait_fill = self.bufs.slice(self.max_readable, None);
        let mut iovs = self.riovs.take().unwrap_or_default();
        iovs.clear();
        let tlen = wait_fill.as_io_slice_mut(&mut iovs);
        let ret = (iovs.as_ptr().cast(), iovs.len() as _, tlen);
        self.riovs = Some(iovs);
        ret
    }

    pub(crate) fn advance_min_readable(&mut self, n: usize) {
        let wait_discard = self.bufs.slice(self.min_readable, None);
        let next = wait_discard.advance_addr(self.min_readable, n).unwrap();
        self.min_readable = next;
        // TODO: free or back to pool for node < self.min_readable
    }

    pub(crate) fn advance_max_readable(&mut self, n: usize) {
        loop {
            let next = self
                .bufs
                .slice(self.max_readable, None)
                .advance_addr(self.max_readable, n);
            if let Some(n) = next {
                self.max_readable = n;
                break;
            }
            self.bufs.append_buf();
        }
    }

    pub(crate) fn recycle(&mut self) {
        self.min_readable = BufAddr::zero();
        self.max_readable = BufAddr::zero();
        let need_clean = self.bufs.node_cnt - self.max_idle_buf_cnt;
        if need_clean > 0 {
            self.bufs.recycle(need_clean);
        }
    }

    pub(crate) fn put_slice(&mut self, src: &[u8]) {
        let filled = self.bufs.put_at(self.max_readable, src);
        self.max_readable = filled;
    }
}

pub struct WriteBuf {
    pub bufs: io_vec::Bufs,
    pub filled: io_vec::BufAddr,
    pub flushed: io_vec::BufAddr,

    pub wiovs: Option<Vec<IoSlice<'static>>>,
    pub max_idle_buf_cnt: usize,
}

impl WriteBuf {
    pub fn new(bufs: io_vec::Bufs, max_idle_buf_cnt: usize) -> Self {
        Self {
            bufs,
            filled: io_vec::BufAddr::zero(),
            flushed: io_vec::BufAddr::zero(),
            wiovs: None,
            max_idle_buf_cnt,
        }
    }

    /// Write a single `Frame` value to the underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
    pub fn write_frame(&mut self, frame: &Frame) {
        // Arrays are encoded by encoding each entry. All other frame types are
        // considered literals. For now, mini-redis is not able to encode
        // recursive frame structures. See below for more details.
        match frame {
            Frame::Array(val) => {
                // Encode the frame type prefix. For an array, it is `*`.
                self.put_slice(b"*");

                // Encode the length of the array.
                self.write_decimal(val.len() as u64);

                // Iterate and encode each entry in the array.
                for entry in &**val {
                    self.write_value(entry);
                }
            }
            // The frame type is a literal. Encode the value directly.
            _ => self.write_value(frame),
        }
    }

    /// Write a frame literal to the stream
    pub fn write_value(&mut self, frame: &Frame) {
        match frame {
            Frame::Simple(val) => {
                self.put_slice(b"+");
                self.put_slice(val.as_bytes());
                self.put_slice(b"\r\n");
            }
            Frame::Error(val) => {
                self.put_slice(b"-");
                self.put_slice(val.as_bytes());
                self.put_slice(b"\r\n");
            }
            Frame::Integer(val) => {
                self.put_slice(b":");
                self.write_decimal(*val);
            }
            Frame::Null => {
                self.put_slice(b"$-1\r\n");
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.put_slice(b"$");
                self.write_decimal(len as u64);
                self.put_slice(val);
                self.put_slice(b"\r\n");
            }
            // Encoding an `Array` from within a value cannot be done using a
            // recursive strategy. In general, async fns do not support
            // recursion. Mini-redis has not needed to encode nested arrays yet,
            // so for now it is skipped.
            Frame::Array(_val) => unreachable!(),
        }
    }

    /// Write a decimal frame to the stream
    pub fn write_decimal(&mut self, val: u64) {
        use std::io::Write;

        // Convert the value to a string
        let mut buf = [0u8; 20];
        let mut buf = std::io::Cursor::new(&mut buf[..]);
        write!(buf, "{}", val).unwrap();

        let pos = buf.position() as usize;
        self.put_slice(&buf.get_ref()[..pos]);
        self.put_slice(b"\r\n");
    }

    #[inline]
    pub fn remain_write(&self) -> bool {
        self.filled != self.flushed
    }

    #[inline]
    pub fn put_slice(&mut self, src: &[u8]) {
        let new_filled = self.bufs.put_at(self.filled, src);
        self.filled = new_filled;
    }

    pub(crate) fn wait_flush_io_slice(&mut self) -> (*const libc::iovec, u32, usize) {
        let mut wait_flush = self.bufs.slice(self.flushed, Some(self.filled));
        let mut iovs = self.wiovs.take().unwrap_or_default();
        iovs.clear();
        let tlen = wait_flush.as_io_slice(&mut iovs);
        let ret = (iovs.as_ptr().cast(), iovs.len() as _, tlen);
        self.wiovs = Some(iovs);
        ret
    }

    pub(crate) fn advance_flush_pos(&mut self, n: usize) {
        let wait_flush = self.bufs.slice(self.flushed, None);
        let flushed = wait_flush.advance_addr(self.flushed, n).unwrap();
        assert!(flushed.buf <= self.filled.buf);
        self.flushed = flushed;
    }

    pub(crate) fn recycle(&mut self) {
        if self.filled == self.flushed && self.filled.buf > 0 {
            // no more inflight write, just reuse from head.
            self.flushed = BufAddr::zero();
            self.filled = BufAddr::zero();
            let need_clean = self.bufs.node_cnt - self.max_idle_buf_cnt;
            if need_clean > 0 {
                self.bufs.recycle(need_clean);
            }
            return;
        }
        if self.flushed.buf >= self.max_idle_buf_cnt {
            // there are inflight writes, remove prefix nodes and rewind pos.
            let recycled = self.bufs.recycle(self.flushed.buf - 2);
            self.flushed.buf -= recycled;
            self.filled.buf -= recycled;
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use std::{cell::RefCell, rc::Rc};

//     use super::*;
//     use crate::io_vec::{IoVec, Pool};

//     #[test]
//     fn test_get_u8_and_has_remain() {
//         let p = Rc::new(RefCell::new(Pool::new(7)));
//         let mut chain = IoVec::new(p, 3);
//         chain.put_slice(b"ebw");
//         let mut c = Cursor::new(&mut chain);
//         let u1 = c.get_u8();
//         assert_eq!(u1, b'e');
//         let u2 = c.get_u8();
//         assert_eq!(u2, b'b');
//         assert!(c.has_remaining());
//         assert_eq!(c.peek_u8(), b'w');
//         let u3 = c.get_u8();
//         assert_eq!(u3, b'w');
//         assert!(!c.has_remaining());
//     }

//     #[test]
//     fn test_get_line() {
//         let p = Rc::new(RefCell::new(Pool::new(7)));
//         let mut chain = IoVec::new(p, 3);
//         chain.put_slice(b"ebw\r\ndafdsf\r\ndsfasdf");
//         let mut c = Cursor::new(&mut chain);
//         {
//             let l1 = c.get_line();
//             assert_eq!(String::from_utf8_lossy(&l1.concat()), "ebw\r\n");
//         }
//         {
//             let l2 = c.get_line();
//             assert_eq!(String::from_utf8_lossy(&l2.concat()), "dafdsf\r\n");
//         }
//         {
//             let l3 = c.get_line();
//             assert_eq!(String::from_utf8_lossy(&l3.concat()), "");
//         }
//     }

//     #[test]
//     fn test_adv() {
//         let p = Rc::new(RefCell::new(Pool::new(7)));
//         let mut chain = IoVec::new(p, 3);
//         chain.put_slice(b"ebw\r\ndafdsf\r\ndsfasdf\r\n");
//         let mut c = Cursor::new(&mut chain);
//         c.advance(1);
//         assert_eq!(c.buf_pos.buf_idx, 0);
//         assert_eq!(c.buf_pos.dat_idx, 1);
//         c.advance(6);
//         assert_eq!(c.buf_pos.buf_idx, 1);
//         assert_eq!(c.buf_pos.dat_idx, 0);
//         c.advance(8);
//         assert_eq!(c.buf_pos.buf_idx, 2);
//         assert_eq!(c.buf_pos.dat_idx, 1);
//     }

//     #[test]
//     fn test_peek_n() {
//         let p = Rc::new(RefCell::new(Pool::new(7)));
//         let mut chain = IoVec::new(p, 3);
//         chain.put_slice(b"ebw11dafdsf22dsfasdf32");
//         let c = Cursor::new(&mut chain);
//         let v1 = c.peek_n(2);
//         assert_eq!(&v1, b"eb");
//         let v1 = c.peek_n(8);
//         assert_eq!(&v1, b"ebw11daf");
//     }
// }
