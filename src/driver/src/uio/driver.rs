use std::{collections::HashMap, io, os::unix::io::RawFd};

use engula_engine::Db;
use io_uring::{cqueue, IoUring};
use tracing::debug;

use super::{Connection, Listener, Token};

pub struct Driver {
    io: Option<IoUring>,
    db: Db,
    last_id: u64,
    listener: Listener,
    connections: HashMap<u64, Connection>,
}

impl Driver {
    pub fn new(fd: RawFd) -> io::Result<Driver> {
        let io = IoUring::new(1024)?;
        let db = Db::default();
        let mut listener = Listener::new(0, fd);
        listener.setup(&io);
        Ok(Self {
            io: Some(io),
            db,
            last_id: 0,
            listener,
            connections: HashMap::new(),
        })
    }

    pub fn tick(&mut self) -> io::Result<()> {
        let io = self.io.take().unwrap();
        io.submit_and_wait(1)?;
        {
            let cq = unsafe { io.completion_shared() };
            for cqe in cq {
                let id = Token(cqe.user_data()).id();
                debug!(id, "completion");
                let cqe = check_cqe_result(cqe);
                if id == self.listener.id() {
                    self.handle_listener(&io, cqe)?;
                } else {
                    self.handle_connection(id, &io, cqe)?;
                }
            }
        }
        self.io = Some(io);
        Ok(())
    }

    fn next_id(&mut self) -> u64 {
        self.last_id += 1;
        self.last_id
    }

    fn handle_listener(&mut self, io: &IoUring, cqe: io::Result<cqueue::Entry>) -> io::Result<()> {
        let id = self.next_id();
        let fd = self.listener.accept(io, cqe)?;
        debug!(fd, "accept");
        let mut conn = Connection::new(id, fd, self.db.clone());
        conn.setup(io);
        self.connections.insert(id, conn);
        Ok(())
    }

    fn handle_connection(
        &mut self,
        id: u64,
        io: &IoUring,
        cqe: io::Result<cqueue::Entry>,
    ) -> io::Result<()> {
        let conn = self.connections.get_mut(&id).unwrap();
        conn.on_tick(io, cqe)?;
        if conn.is_done() {
            self.connections.remove(&id);
        }
        Ok(())
    }
}

fn check_cqe_result(cqe: cqueue::Entry) -> io::Result<cqueue::Entry> {
    let res = cqe.result();
    if res >= 0 {
        Ok(cqe)
    } else {
        Err(io::Error::from_raw_os_error(-res))
    }
}
