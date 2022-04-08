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
    collections::VecDeque,
    io::{self, Cursor, ErrorKind, IoSlice},
};

use bytes::{Buf, BytesMut};
use smallvec::SmallVec;
use tokio::net::TcpStream;

use crate::{Frame, FrameError};

const WRITE_BUFFER_CAPACITY: usize = 16 * 1024;

/// Send and receive `Frame` values from a remote peer.
///
/// When implementing networking protocols, a message on that protocol is
/// often composed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
///
/// To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,

    // The buffer for reading frames.
    buffer: BytesMut,

    // The buffers for writing frames.
    interest_writable: bool,
    reply_buffer: Cursor<Vec<u8>>,
    reply_buffer_queue: VecDeque<Cursor<Vec<u8>>>,
}

impl Connection {
    /// Create a new `Connection`, backed by `stream`. Read and write buffers
    /// are initialized.
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            // Default to a 4KB read buffer. For the use case of mini redis,
            // this is fine. However, real applications will want to tune this
            // value to their specific use case. There is a high likelihood that
            // a larger read buffer will work better.
            buffer: BytesMut::with_capacity(4 * 1024),
            interest_writable: false,
            reply_buffer: Cursor::new(Vec::with_capacity(WRITE_BUFFER_CAPACITY)),
            reply_buffer_queue: VecDeque::new(),
        }
    }

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            self.flush()?;

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            if 0 == self.read_buf().await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use FrameError::Incomplete;

        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&self.buffer[..]);

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
                let frame = Frame::parse(&mut buf)?;

                // Discard the parsed data from the read buffer.
                //
                // When `advance` is called on the read buffer, all of the data
                // up to `len` is discarded. The details of how this works is
                // left to `BytesMut`. This is often done by moving an internal
                // cursor, but it may be done by reallocating and copying data.
                self.buffer.advance(len);

                // Return the parsed frame to the caller.
                Ok(Some(frame))
            }
            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(Incomplete) => Ok(None),
            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(e) => Err(e.into()),
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
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        // Arrays are encoded by encoding each entry. All other frame types are
        // considered literals. For now, mini-redis is not able to encode
        // recursive frame structures. See below for more details.
        match frame {
            Frame::Array(val) => {
                // Encode the frame type prefix. For an array, it is `*`.
                self.write_u8(b'*')?;

                // Encode the length of the array.
                self.write_decimal(val.len() as u64)?;

                // Iterate and encode each entry in the array.
                for entry in &**val {
                    self.write_value(entry)?;
                }
            }
            // The frame type is a literal. Encode the value directly.
            _ => self.write_value(frame)?,
        }

        Ok(())
    }

    /// Write a frame literal to the stream
    fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.write_u8(b'+')?;
                self.write_all(val.as_bytes())?;
                self.write_all(b"\r\n")?;
            }
            Frame::Error(val) => {
                self.write_u8(b'-')?;
                self.write_all(val.as_bytes())?;
                self.write_all(b"\r\n")?;
            }
            Frame::Integer(val) => {
                self.write_u8(b':')?;
                self.write_decimal(*val)?;
            }
            Frame::Null => {
                self.write_all(b"$-1\r\n")?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.write_u8(b'$')?;
                self.write_decimal(len as u64)?;
                self.write_all(val)?;
                self.write_all(b"\r\n")?;
            }
            // Encoding an `Array` from within a value cannot be done using a
            // recursive strategy. In general, async fns do not support
            // recursion. Mini-redis has not needed to encode nested arrays yet,
            // so for now it is skipped.
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    /// Write a decimal frame to the stream
    fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // Convert the value to a string
        let mut buf = [0u8; 22];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(buf, "{}\r\n", val)?;

        let pos = buf.position() as usize;
        self.write_all(&buf.get_ref()[..pos])?;

        Ok(())
    }

    #[inline]
    fn write_u8(&mut self, val: u8) -> io::Result<()> {
        self.write_all(&[val])
    }

    fn write_all(&mut self, mut val: &[u8]) -> io::Result<()> {
        use std::io::Write;

        while !val.is_empty() {
            let buf = self.reply_buffer.get_mut();
            let avail = buf.capacity().saturating_sub(buf.len());
            if val.len() <= avail {
                Write::write_all(buf, val)?;
                break;
            }

            Write::write_all(buf, &val[..avail])?;
            val = &val[avail..];
            self.flush()?;
        }
        Ok(())
    }

    // Ensure the encoded frame is written to the socket. The calls above
    // are to the buffered writes. Calling `flush` writes the remaining contents of the buffer to
    // the socket.
    fn flush(&mut self) -> io::Result<()> {
        if !self.interest_writable {
            if self.reply_buffer.is_empty() {
                return Ok(());
            }

            self.flush_buf()?;
            if self.reply_buffer.is_empty() {
                // All buffered bytes are written.
                self.reply_buffer.set_position(0);
                self.reply_buffer.get_mut().clear();
                debug_assert!(self.reply_buffer.is_empty());
                return Ok(());
            }

            // The underlying stream returns [`ErrorKind::WouldBlock`].
            self.interest_writable = true;
        }

        // Do not swap reply buffer if it has avail space.
        let buf = self.reply_buffer.get_mut();
        if buf.len() == buf.capacity() && !self.reply_buffer.is_empty() {
            let mut new_reply_buffer = Cursor::new(Vec::with_capacity(WRITE_BUFFER_CAPACITY));
            std::mem::swap(&mut new_reply_buffer, &mut self.reply_buffer);
            self.reply_buffer_queue.push_back(new_reply_buffer);
        }
        Ok(())
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        let buf = &mut self.reply_buffer;
        while !buf.is_empty() {
            match self.stream.try_write(buf.remaining_slice()) {
                Ok(n) => buf.set_position(buf.position() + n as u64),
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn read_buf(&mut self) -> io::Result<usize> {
        match self.stream.try_read_buf(&mut self.buffer) {
            Ok(num) => Ok(num),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => self.wait_read_ready().await,
            Err(e) => Err(e),
        }
    }

    async fn wait_read_ready(&mut self) -> io::Result<usize> {
        use tokio::io::Interest;

        loop {
            let mut interest = Interest::READABLE;
            if self.interest_writable {
                interest |= Interest::WRITABLE;
            }
            let ready = self.stream.ready(interest).await?;
            if ready.is_writable() {
                self.continue_writes().await?;
            }

            if ready.is_readable() {
                return self.stream.try_read_buf(&mut self.buffer);
            }
        }
    }

    async fn continue_writes(&mut self) -> io::Result<()> {
        const MAX_VECTORED_IO: usize = 8;

        // FIXME(walter) the underlying `std::net::TcpStream` use `send` instead of `writev`.
        while !self.reply_buffer_queue.is_empty() || self.reply_buffer.is_empty() {
            let mut vectored: SmallVec<[IoSlice; MAX_VECTORED_IO]> = SmallVec::new();
            if !self.reply_buffer_queue.is_empty() {
                for slice in self
                    .reply_buffer_queue
                    .iter()
                    .map(|buf| buf.remaining_slice())
                    .take(MAX_VECTORED_IO)
                {
                    vectored.push(IoSlice::new(slice));
                }
            }
            if vectored.len() != MAX_VECTORED_IO && self.reply_buffer.is_empty() {
                vectored.push(IoSlice::new(self.reply_buffer.remaining_slice()));
            }

            let mut num_written = match self.stream.try_write_vectored(&vectored) {
                Ok(n) => n,
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e),
            };
            drop(vectored);

            while let Some(buf) = self.reply_buffer_queue.front_mut() {
                let remaining_len = buf.remaining_slice().len();
                if remaining_len > num_written {
                    buf.set_position(buf.position() + num_written as u64);
                    num_written = 0;
                    break;
                }
                num_written -= remaining_len;
                self.reply_buffer_queue.pop_front();
            }
            if num_written > 0 {
                let remaining_len = self.reply_buffer.remaining_slice().len();
                if remaining_len > num_written {
                    self.reply_buffer
                        .set_position(self.reply_buffer.position() + num_written as u64);
                } else {
                    self.reply_buffer.get_mut().clear();
                    self.reply_buffer.set_position(0);
                }
            }
        }

        // All buffered bytes are written.
        debug_assert!(self.reply_buffer_queue.is_empty());
        debug_assert!(self.reply_buffer.is_empty());
        self.interest_writable = false;

        Ok(())
    }
}
