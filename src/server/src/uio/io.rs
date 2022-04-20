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

use std::{io, rc::Rc};

use io_uring::{opcode, squeue, types, CompletionQueue, IoUring};

use crate::Result;

#[derive(Clone)]
pub struct IoDriver {
    io: Rc<IoUring>,
}

impl IoDriver {
    pub fn new() -> Result<IoDriver> {
        let io = IoUring::new(4096)?;
        Ok(Self { io: Rc::new(io) })
    }

    pub fn wait(&mut self, want: usize) -> Result<usize> {
        let io = unsafe { Rc::get_mut_unchecked(&mut self.io) };
        let ts = types::Timespec::new().sec(1);
        let timeout = opcode::Timeout::new(&ts).build();
        unsafe {
            io.submission().push(&timeout)?;
        }
        let n = io.submit_and_wait(want)?;
        Ok(n)
    }

    pub fn enqueue(&mut self, sqe: squeue::Entry) -> Result<()> {
        let io = unsafe { Rc::get_mut_unchecked(&mut self.io) };
        let mut sq = unsafe { io.submission_shared() };
        while sq.is_full() {
            io.submit_and_wait(1)?;
            sq.sync();
        }
        unsafe {
            sq.push(&sqe).unwrap();
        }
        Ok(())
    }

    pub fn dequeue(&mut self) -> CompletionQueue<'_> {
        let io = unsafe { Rc::get_mut_unchecked(&mut self.io) };
        let cq = io.completion();
        assert_eq!(cq.overflow(), 0);
        cq
    }
}

pub fn check_io_result(res: i32) -> io::Result<i32> {
    if res >= 0 {
        Ok(res)
    } else {
        Err(io::Error::from_raw_os_error(-res))
    }
}
