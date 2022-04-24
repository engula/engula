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
    io::{Read, Write},
    rc::Rc,
    time::{Duration, Instant},
};

use engula_engine::Db;
use mio::{event::Event, net::TcpStream};
use tracing::trace;

use super::{interrupted, would_block};
use crate::{
    cmd,
    io_vec::{self, Pool},
    Error, ReadBuf, Result, WriteBuf,
};

pub struct Connection {
    db: Db,
    stream: TcpStream,
    read_buf: ReadBuf,
    write_buf: WriteBuf,
    last_interaction: Instant,
}

impl Connection {
    pub fn new(db: Db, stream: TcpStream) -> Connection {
        let pool = Rc::new(RefCell::new(Pool::new(4 * 1024)));
        Self {
            db,
            read_buf: ReadBuf::new(io_vec::Bufs::new(pool.clone(), 2), 2),
            write_buf: WriteBuf::new(io_vec::Bufs::new(pool, 2), 2),
            stream,
            last_interaction: Instant::now(),
        }
    }

    pub fn stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    pub fn elapsed_from_last_interation(&self) -> Duration {
        self.last_interaction.elapsed()
    }

    pub fn handle_connection_event(&mut self, event: &Event) -> Result<bool> {
        trace!("handle_connection_event {:?}", event);

        self.last_interaction = Instant::now();

        if event.is_readable() {
            let mut connection_closed = false;
            let mut received_data = vec![0; 4096];
            let mut bytes_read = 0;
            // We can (maybe) read from the connection.
            loop {
                match self.stream.read(&mut received_data[bytes_read..]) {
                    Ok(0) => {
                        // Reading 0 bytes means the other side has closed the
                        // connection or is done writing, then so are we.
                        connection_closed = true;
                        break;
                    }
                    Ok(n) => bytes_read += n,
                    // Would block "errors" are the OS's way of saying that the
                    // connection is not actually ready to perform this I/O operation.
                    Err(ref e) if would_block(e) => break,
                    Err(ref e) if interrupted(e) => continue,
                    // Other errors we'll consider fatal.
                    Err(e) => return Err(Error::Io(e)),
                }
            }

            if bytes_read != 0 {
                self.read_buf.put_slice(&received_data[..bytes_read]);
                while let Some(frame) = self.read_buf.parse_frame() {
                    self.write_buf.write_frame(&cmd::apply(frame, &self.db));
                }
                self.read_buf.recycle();
                self.write_buf.recycle();
            }

            if connection_closed {
                trace!("Connection closed");
                return Ok(true);
            }
        }

        if self.write_buf.remain_write() {
            let _ = &mut self.write_buf.wait_flush_io_slice();
            let iovs = self.write_buf.wiovs.as_ref().unwrap().as_slice();
            match self.stream.write_vectored(iovs) {
                Ok(n) => self.write_buf.advance_flush_pos(n),
                Err(ref e) if would_block(e) => return Ok(false),
                Err(ref e) if interrupted(e) => return self.handle_connection_event(event),
                Err(e) => return Err(Error::Io(e)),
            }
        }

        Ok(false)
    }
}
