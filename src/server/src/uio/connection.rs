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

use std::os::unix::io::RawFd;

use bytes::BufMut;
use engula_engine::Db;
use io_uring::{cqueue, opcode, squeue, types};
use tracing::{error, trace};

use super::{check_io_result, IoDriver, Token};
use crate::{Command, ReadBuf, Result, WriteBuf};

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
            rbuf: ReadBuf::default(),
            wbuf: WriteBuf::default(),
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