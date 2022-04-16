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

use std::{os::unix::io::RawFd, ptr};

use io_uring::{cqueue, opcode, types};
use tracing::{error, trace};

use super::{check_io_result, IoDriver, Token};
use crate::Result;

pub struct Listener {
    id: u64,
    fd: RawFd,
    io: IoDriver,
}

impl Listener {
    pub fn new(id: u64, fd: RawFd, io: IoDriver) -> Listener {
        Self { id, fd, io }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn prepare_accept(&mut self) -> Result<()> {
        let token = Token::new(self.id, opcode::Accept::CODE);
        let sqe = opcode::Accept::new(types::Fd(self.fd), ptr::null_mut(), ptr::null_mut())
            .build()
            .user_data(token.0);
        match self.io.enqueue(sqe) {
            Ok(_) => {
                trace!("listener prepare accept");
                Ok(())
            }
            Err(err) => {
                error!(%err, "listener prepare accept");
                Err(err)
            }
        }
    }

    pub fn complete_accept(&mut self, cqe: cqueue::Entry) -> Result<RawFd> {
        match check_io_result(cqe.result()) {
            Ok(fd) => {
                trace!("listener complete accept");
                Ok(fd)
            }
            Err(err) => {
                error!(%err, "listener complete accept");
                Err(err.into())
            }
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}
