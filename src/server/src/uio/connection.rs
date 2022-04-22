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
    os::unix::io::RawFd,
    rc::Rc,
    time::{Duration, Instant},
};

use engula_engine::Db;
use io_uring::{cqueue, opcode, squeue, types};
use tracing::{error, trace};

use super::{check_io_result, IoDriver, Token};
use crate::{cmd, io_vec, Pool, ReadBuf, Result, WriteBuf};

pub struct Connection {
    id: u64,
    fd: RawFd,
    io: IoDriver,
    db: Db,
    read_buf: ReadBuf,
    write_buf: WriteBuf,
    last_interaction: Instant,
    has_read: bool,
    has_write: bool,
    num_inflights: usize,
}

impl Connection {
    pub fn new(id: u64, fd: RawFd, io: IoDriver, db: Db) -> Connection {
        let pool = Rc::new(RefCell::new(Pool::new(4 * 1024)));
        let mut conn = Self {
            id,
            fd,
            io,
            db,
            read_buf: ReadBuf::new(io_vec::Bufs::new(pool.clone(), 2)),
            write_buf: WriteBuf::new(io_vec::Bufs::new(pool, 2)),
            last_interaction: Instant::now(),
            has_read: false,
            has_write: false,
            num_inflights: 0,
        };
        conn.prepare_read();
        conn
    }

    pub fn elapsed_from_last_interation(&self) -> Duration {
        self.last_interaction.elapsed()
    }

    // Handles a completion event.
    //
    // Returns true if the connection should be dropped.
    pub fn tick(&mut self, cqe: cqueue::Entry) -> bool {
        self.num_inflights -= 1;
        self.last_interaction = Instant::now();

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

    fn process(&mut self) {
        while let Some(frame) = self.read_buf.parse_frame() {
            self.write_buf.write_frame(&cmd::apply(frame, &self.db));
        }
    }

    fn prepare(&mut self, sqe: squeue::Entry) -> Result<()> {
        self.io.enqueue(sqe)?;
        self.num_inflights += 1;
        Ok(())
    }

    fn prepare_read(&mut self) {
        let token = Token::new(self.id, opcode::Readv::CODE);
        let (iovs, iovs_len) = self.read_buf.wait_fill_io_slice_mut();
        let sqe = opcode::Readv::new(types::Fd(self.fd), iovs, iovs_len)
            .build()
            .user_data(token.0);
        match self.prepare(sqe) {
            Ok(_) => {
                trace!(self.id, "connection prepare read");
                self.has_read = true;
            }
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

        self.read_buf.advance_max_readable(size);

        self.has_read = false;

        self.process();

        self.read_buf.recycle();

        if !self.has_read {
            self.prepare_read();
        }

        if !self.has_write && self.write_buf.remain_write() {
            self.prepare_write();
        }
    }

    fn prepare_write(&mut self) {
        let token = Token::new(self.id, opcode::Writev::CODE);
        let (iovs, iovs_len) = self.write_buf.wait_flush_io_slice();
        let sqe = opcode::Writev::new(types::Fd(self.fd), iovs, iovs_len)
            .build()
            .user_data(token.0);
        match self.prepare(sqe) {
            Ok(_) => {
                trace!(self.id, "connection prepare write");
                self.has_write = true;
            }
            Err(err) => error!(%err, self.id, "connection prepare write"),
        }
    }

    fn complete_write(&mut self, size: usize) {
        self.write_buf.advance_flush_pos(size);
        self.write_buf.recycle();
        self.has_write = false;
        if !self.has_write && self.write_buf.remain_write() {
            self.prepare_write();
        }
        if !self.has_read {
            self.prepare_read();
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
