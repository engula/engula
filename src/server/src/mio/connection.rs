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

use std::io::Read;

use engula_engine::Db;
use mio::{event::Event, net::TcpStream};
use tracing::info;

use super::{interrupted, would_block};
use crate::{Command, Error, ReadBuf, Result, WriteBuf};

pub struct Connection {
    db: Db,
    stream: TcpStream,
    read_buf: ReadBuf,
    write_buf: WriteBuf,
}

impl Connection {
    pub fn new(db: Db, stream: TcpStream) -> Connection {
        Self {
            db,
            read_buf: ReadBuf::default(),
            write_buf: WriteBuf::default(),
            stream,
        }
    }

    pub fn stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    pub fn handle_connection_event(&mut self, event: &Event) -> Result<bool> {
        info!("handle_connection_event {:?}", event);
        if event.is_readable() {
            let mut connection_closed = false;
            let mut received_data = vec![0; 4096];
            let mut bytes_read = 0;
            // We can (maybe) read from the connection.
            loop {
                match self.stream.read(&mut received_data[bytes_read..]) {
                    Ok(0) => {
                        info!("receive 0");
                        // Reading 0 bytes means the other side has closed the
                        // connection or is done writing, then so are we.
                        connection_closed = true;
                        break;
                    }
                    Ok(n) => {
                        info!("receive n: {}", n);
                        bytes_read += n;
                    }
                    // Would block "errors" are the OS's way of saying that the
                    // connection is not actually ready to perform this I/O operation.
                    Err(ref e) if would_block(e) => break,
                    Err(ref e) if interrupted(e) => continue,
                    // Other errors we'll consider fatal.
                    Err(e) => return Err(Error::Io(e)),
                }
            }

            if bytes_read != 0 {
                // while let Some(frame) = self.read_buf.parse_frame() {
                //     info!("read frame: {:?}", frame);
                //     let cmd = Command::from_frame(frame).unwrap();
                //     let reply = cmd.apply(&self.db).unwrap();
                //     self.write_buf.write_frame(&reply);
                // }
            }

            if connection_closed {
                info!("Connection closed");
                return Ok(true);
            }
        }
        Ok(false)
    }
}
