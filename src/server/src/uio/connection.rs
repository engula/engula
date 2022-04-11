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
    net::TcpStream,
    os::unix::io::{FromRawFd, RawFd},
};

use io_uring::{cqueue, opcode, squeue, types};
use tracing::{error, trace};

use super::{check_io_result, IoDriver, IoVec, Token};
use crate::Result;

pub struct Connection {
    id: u64,
    fd: RawFd,
    io: IoDriver,
    rbuf: IoVec,
    wbuf: IoVec,
    num_inflights: usize,
}

impl Connection {
    pub fn new(id: u64, fd: RawFd, io: IoDriver) -> Result<Connection> {
        let tcp = unsafe { TcpStream::from_raw_fd(fd) };
        tcp.set_nodelay(true)?;
        let mut conn = Self {
            id,
            fd,
            io,
            rbuf: IoVec::new(),
            wbuf: IoVec::new(),
            num_inflights: 0,
        };
        conn.prepare_read();
        Ok(conn)
    }

    // Handles a completion event.
    //
    // Returns true if the connection should be dropped.
    pub fn tick(&mut self, cqe: cqueue::Entry) -> bool {
        self.num_inflights -= 1;

        let op = Token::op(cqe.user_data());
        let result = check_io_result(cqe.result());
        match op {
            opcode::Readv::CODE => match result {
                Ok(size) => {
                    trace!(self.id, size, "connection complete read");
                    self.complete_read(size as usize);
                }
                Err(err) => error!(%err, self.id, "connection complete read"),
            },
            opcode::Writev::CODE => match result {
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

    fn prepare(&mut self, sqe: squeue::Entry) -> Result<()> {
        self.io.enqueue(sqe)?;
        self.num_inflights += 1;
        Ok(())
    }

    fn prepare_read(&mut self) {
        let token = Token::new(self.id, opcode::Readv::CODE);
        let iovec = self.rbuf.as_raw();
        let sqe = opcode::Readv::new(types::Fd(self.fd), iovec.0, iovec.1)
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

        self.rbuf.advance(size);
        self.prepare_read();
    }

    fn prepare_write(&mut self) {
        let token = Token::new(self.id, opcode::Writev::CODE);
        let iovec = self.wbuf.as_raw();
        let sqe = opcode::Writev::new(types::Fd(self.fd), iovec.0, iovec.1)
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
