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

use std::{io::Cursor, os::unix::io::RawFd};

use bytes::{Buf, BufMut, BytesMut};
use engula_engine::Db;
use io_uring::{cqueue, opcode, squeue, types};
use tracing::{error, trace};

use super::{check_io_result, IoDriver, Token};
use crate::{Command, Frame, FrameError, Result};

pub struct Connection {
    id: u64,
    fd: RawFd,
    io: IoDriver,
    db: Db,
    rbuf: ReadBuf,
    wbuf: WriteBuf,
    num_inflights: usize,
}

impl Connection {
    pub fn new(id: u64, fd: RawFd, io: IoDriver, db: Db) -> Connection {
        let mut conn = Self {
            id,
            fd,
            io,
            db,
            rbuf: ReadBuf::new(),
            wbuf: WriteBuf::new(),
            num_inflights: 0,
        };
        conn.prepare_read();
        conn
    }

    // Handles a completion event.
    //
    // Returns true if the connection should be dropped.
    pub fn tick(&mut self, cqe: cqueue::Entry) -> bool {
        self.num_inflights -= 1;

        let op = Token::op(cqe.user_data());
        let result = check_io_result(cqe.result());
        match op {
            opcode::Read::CODE => match result {
                Ok(size) => {
                    trace!(self.id, size, "connection complete read");
                    self.complete_read(size as usize);
                }
                Err(err) => error!(%err, self.id, "connection complete read"),
            },
            opcode::Write::CODE => match result {
                Ok(size) => {
                    trace!(self.id, size, "connection complete write");
                    self.complete_write(size as usize);
                }
                Err(err) => error!(%err, self.id, "connection complete write"),
            },
            opcode::Close::CODE => match result {
                Ok(_) => trace!(self.id, "connection complete close"),
                Err(err) => error!(%err, self.id, "connection complete close"),
            },
            _ => unreachable!(),
        }

        self.num_inflights == 0
    }

    fn process(&mut self) {
        while let Some(frame) = self.rbuf.parse_frame() {
            let cmd = Command::from_frame(frame).unwrap();
            let reply = cmd.apply(&self.db).unwrap();
            self.wbuf.write_frame(&reply);
        }
    }

    fn prepare(&mut self, sqe: squeue::Entry) -> Result<()> {
        self.io.enqueue(sqe)?;
        self.num_inflights += 1;
        Ok(())
    }

    fn prepare_read(&mut self) {
        let token = Token::new(self.id, opcode::Read::CODE);
        let buf = self.rbuf.buf.chunk_mut();
        let sqe = opcode::Read::new(types::Fd(self.fd), buf.as_mut_ptr(), buf.len() as u32)
            .build()
            .user_data(token.0);
        match self.prepare(sqe) {
            Ok(_) => trace!(self.id, "connection prepare read"),
            Err(err) => error!(%err, self.id, "connection prepare read"),
        }
    }

    fn complete_read(&mut self, size: usize) {
        // We never read with zero-length buffer. So if the read returns zero, it means that the
        // connection is closed.
        if size == 0 {
            self.prepare_close();
            return;
        }

        unsafe {
            self.rbuf.buf.advance_mut(size);
        }

        self.process();

        self.prepare_read();
        if !self.wbuf.is_empty() {
            self.prepare_write();
        }
    }

    fn prepare_write(&mut self) {
        let token = Token::new(self.id, opcode::Write::CODE);
        let buf = &mut self.wbuf.buf[self.wbuf.written..];
        let sqe = opcode::Write::new(types::Fd(self.fd), buf.as_mut_ptr(), buf.len() as u32)
            .build()
            .user_data(token.0);
        match self.prepare(sqe) {
            Ok(_) => trace!(self.id, "connection prepare write"),
            Err(err) => error!(%err, self.id, "connection prepare write"),
        }
    }

    fn complete_write(&mut self, size: usize) {
        self.wbuf.consume(size);
        if !self.wbuf.is_empty() {
            self.prepare_write();
        }
    }

    fn prepare_close(&mut self) {
        let token = Token::new(self.id, opcode::Close::CODE);
        let sqe = opcode::Close::new(types::Fd(self.fd))
            .build()
            .user_data(token.0);
        match self.prepare(sqe) {
            Ok(_) => trace!(self.id, "connection prepare close"),
            Err(err) => error!(%err, self.id, "connection prepare close"),
        }
    }
}

struct ReadBuf {
    buf: BytesMut,
}

impl ReadBuf {
    fn new() -> ReadBuf {
        Self {
            buf: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&mut self) -> Option<Frame> {
        use FrameError::Incomplete;

        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&self.buf[..]);

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
                let len = buf.position() as usize;

                // Reset the position to zero before passing the cursor to
                // `Frame::parse`.
                buf.set_position(0);

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
                self.buf.advance(len);

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
            Err(_) => panic!(),
        }
    }
}

struct WriteBuf {
    buf: BytesMut,
    written: usize,
}

impl WriteBuf {
    fn new() -> WriteBuf {
        Self {
            buf: BytesMut::with_capacity(4 * 1024),
            written: 0,
        }
    }

    fn consume(&mut self, size: usize) {
        self.written += size;
        if self.written == self.buf.len() {
            self.buf.clear();
            self.written = 0;
        }
    }

    fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Write a single `Frame` value to the underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
    fn write_frame(&mut self, frame: &Frame) {
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
    fn write_value(&mut self, frame: &Frame) {
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
    fn write_decimal(&mut self, val: u64) {
        use std::io::Write;

        // Convert the value to a string
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(buf, "{}", val).unwrap();

        let pos = buf.position() as usize;
        self.buf.put_slice(&buf.get_ref()[..pos]);
        self.buf.put_slice(b"\r\n");
    }
}
