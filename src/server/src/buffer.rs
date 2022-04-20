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

use crate::{io_vec, Frame, FrameError};

#[derive(Default)]
struct BufPos {
    buf_idx: usize,
    dat_idx: usize,
}

pub struct Cursor<'b> {
    pub chain: &'b mut io_vec::BufChain,
    buf_pos: BufPos,
    offset: usize,
}

impl<'b> Cursor<'b> {
    pub fn new(buf_chain: &'b mut io_vec::BufChain) -> Self {
        Self {
            chain: buf_chain,
            buf_pos: Default::default(),
            offset: 0,
        }
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn reset(&mut self) {
        self.buf_pos = Default::default();
        self.offset = 0;
    }

    pub fn has_remaining(&self) -> bool {
        self.offset < self.chain.data_remain()
    }

    pub fn get_u8(&mut self) -> u8 {
        let buf = self
            .chain
            .as_consume_read_view()
            .nth(self.buf_pos.buf_idx)
            .unwrap();
        let res = buf[self.buf_pos.dat_idx];
        self.buf_pos.dat_idx += 1;
        self.offset += 1;
        if self.buf_pos.dat_idx == buf.len() {
            self.buf_pos.dat_idx = 0;
            self.buf_pos.buf_idx += 1;
        }
        res
    }

    pub fn peek_u8(&mut self) -> u8 {
        let buf = self
            .chain
            .as_consume_read_view()
            .nth(self.buf_pos.buf_idx)
            .unwrap();
        buf[self.buf_pos.dat_idx]
    }

    pub(crate) fn get_line<'a>(&'a mut self) -> Vec<&'a [u8]> {
        const WAIT_R: u8 = 1;
        const WAIT_N: u8 = 2;

        let mut iovs = self.chain.as_consume_read_view().skip(self.buf_pos.buf_idx);

        let mut buf_idx = self.buf_pos.buf_idx;
        let mut data_idx = self.buf_pos.dat_idx;

        let mut get_line_state = WAIT_R;
        let mut lines = Vec::with_capacity(2);
        let mut new_offset = self.offset;
        while let Some(buf) = iovs.next() {
            if buf.is_empty() {
                return Vec::new();
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
                        lines.push(&buf[data_idx..i + 1]);
                        new_offset += i + 1 - data_idx;
                        self.buf_pos = if i + 1 == buf.len() {
                            BufPos {
                                buf_idx: buf_idx + 1,
                                dat_idx: 0,
                            }
                        } else {
                            BufPos {
                                buf_idx,
                                dat_idx: i + 1,
                            }
                        };
                        self.offset = new_offset;
                        return lines;
                    }
                    _ => unreachable!(),
                }
            }
            lines.push(&buf[data_idx..buf.len()]);
            new_offset += buf.len() - data_idx;
            data_idx = 0;
            buf_idx += 1;
        }
        Vec::new()
    }

    pub(crate) fn remaining(&self) -> usize {
        self.chain.data_remain() - self.offset
    }

    pub(crate) fn advance(&mut self, n: usize) {
        let BufPos {
            mut buf_idx,
            mut dat_idx,
        } = self.buf_pos;

        let mut iter = self.chain.as_consume_read_view().skip(buf_idx);
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
        self.buf_pos = BufPos { buf_idx, dat_idx };
        self.offset += n;
    }

    pub(crate) fn peek_n(&self, n: usize) -> Vec<u8> {
        let BufPos {
            mut buf_idx,
            mut dat_idx,
        } = self.buf_pos;
        let mut iovs = self.chain.as_consume_read_view().skip(buf_idx);
        let mut remain_n = n;

        let mut v = Vec::with_capacity(n); // TODO: reduce alloc or reuse.
        loop {
            let buf = iovs.next().unwrap();
            if dat_idx + remain_n > buf.len() {
                v.extend(&buf[dat_idx..]);
                remain_n = remain_n + dat_idx - buf.len();
                buf_idx += 1;
                dat_idx = 0;
            } else {
                v.extend(&buf[dat_idx..dat_idx + remain_n]);
                break;
            }
        }
        v
    }
}

pub struct ReadBuf {
    pub buf: io_vec::BufChain,
}

impl ReadBuf {
    pub fn new(iv: io_vec::BufChain) -> Self {
        Self { buf: iv }
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
        let mut buf = Cursor::new(&mut self.buf);

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
                self.buf.advance_rpos(len);

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
}

pub struct WriteBuf {
    pub buf: io_vec::BufChain,
}

impl WriteBuf {
    pub fn new(iv: io_vec::BufChain) -> Self {
        Self { buf: iv }
    }

    pub fn remain_write(&self) -> bool {
        self.buf.data_remain() > 0
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
                self.buf.put_slice(b"*");

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
                self.buf.put_slice(b"+");
                self.buf.put_slice(val.as_bytes());
                self.buf.put_slice(b"\r\n");
            }
            Frame::Error(val) => {
                self.buf.put_slice(b"-");
                self.buf.put_slice(val.as_bytes());
                self.buf.put_slice(b"\r\n");
            }
            Frame::Integer(val) => {
                self.buf.put_slice(b":");
                self.write_decimal(*val);
            }
            Frame::Null => {
                self.buf.put_slice(b"$-1\r\n");
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.buf.put_slice(b"$");
                self.write_decimal(len as u64);
                self.buf.put_slice(val);
                self.buf.put_slice(b"\r\n");
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
        self.buf.put_slice(&buf.get_ref()[..pos]);
        self.buf.put_slice(b"\r\n");
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use super::*;
    use crate::io_vec::{BufChain, Pool};

    #[test]
    fn test_get_u8_and_has_remain() {
        let p = Rc::new(RefCell::new(Pool::new(7)));
        let mut chain = BufChain::new(p, 3);
        chain.put_slice(b"ebw");
        let mut c = Cursor::new(&mut chain);
        let u1 = c.get_u8();
        assert_eq!(u1, b'e');
        let u2 = c.get_u8();
        assert_eq!(u2, b'b');
        assert!(c.has_remaining());
        assert_eq!(c.peek_u8(), b'w');
        let u3 = c.get_u8();
        assert_eq!(u3, b'w');
        assert!(!c.has_remaining());
    }

    #[test]
    fn test_get_line() {
        let p = Rc::new(RefCell::new(Pool::new(7)));
        let mut chain = BufChain::new(p, 3);
        chain.put_slice(b"ebw\r\ndafdsf\r\ndsfasdf");
        let mut c = Cursor::new(&mut chain);
        {
            let l1 = c.get_line();
            assert_eq!(String::from_utf8_lossy(&l1.concat()), "ebw\r\n");
        }
        {
            let l2 = c.get_line();
            assert_eq!(String::from_utf8_lossy(&l2.concat()), "dafdsf\r\n");
        }
        {
            let l3 = c.get_line();
            assert_eq!(String::from_utf8_lossy(&l3.concat()), "");
        }
    }

    #[test]
    fn test_adv() {
        let p = Rc::new(RefCell::new(Pool::new(7)));
        let mut chain = BufChain::new(p, 3);
        chain.put_slice(b"ebw\r\ndafdsf\r\ndsfasdf\r\n");
        let mut c = Cursor::new(&mut chain);
        c.advance(1);
        assert_eq!(c.buf_pos.buf_idx, 0);
        assert_eq!(c.buf_pos.dat_idx, 1);
        c.advance(6);
        assert_eq!(c.buf_pos.buf_idx, 1);
        assert_eq!(c.buf_pos.dat_idx, 0);
        c.advance(8);
        assert_eq!(c.buf_pos.buf_idx, 2);
        assert_eq!(c.buf_pos.dat_idx, 1);
    }

    #[test]
    fn test_peek_n() {
        let p = Rc::new(RefCell::new(Pool::new(7)));
        let mut chain = BufChain::new(p, 3);
        chain.put_slice(b"ebw11dafdsf22dsfasdf32");
        let c = Cursor::new(&mut chain);
        let v1 = c.peek_n(2);
        assert_eq!(&v1, b"eb");
        let v1 = c.peek_n(8);
        assert_eq!(&v1, b"ebw11daf");
    }
}
