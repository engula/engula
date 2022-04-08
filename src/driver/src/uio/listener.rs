use std::{
    io,
    mem::{size_of, MaybeUninit},
    os::unix::io::RawFd,
};

use io_uring::{cqueue, opcode, types, IoUring};

use super::Token;

pub struct Listener {
    id: u64,
    fd: RawFd,
    addr: MaybeUninit<libc::sockaddr_storage>,
    addrlen: libc::socklen_t,
}

impl Listener {
    pub fn new(id: u64, fd: RawFd) -> Listener {
        Self {
            id,
            fd,
            addr: MaybeUninit::uninit(),
            addrlen: size_of::<libc::sockaddr_storage>() as _,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn setup(&mut self, io: &IoUring) {
        let token = Token::new(self.id, opcode::Accept::CODE);
        let sqe = opcode::Accept::new(
            types::Fd(self.fd),
            self.addr.as_mut_ptr() as *mut _,
            &mut self.addrlen,
        )
        .build()
        .user_data(token.0);
        let mut sq = unsafe { io.submission_shared() };
        if sq.is_full() {
            io.submit().unwrap();
        }
        unsafe {
            sq.push(&sqe).unwrap();
        }
    }

    pub fn accept(&mut self, io: &IoUring, cqe: io::Result<cqueue::Entry>) -> io::Result<RawFd> {
        self.setup(io);
        cqe.map(|e| e.result())
    }
}
