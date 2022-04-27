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
    collections::HashMap,
    net::{TcpListener, TcpStream},
    os::unix::io::{FromRawFd, IntoRawFd},
    time::Duration,
};

use engula_engine::Db;
use io_uring::cqueue;
use tracing::{error, info, trace};

use super::{Connection, IoDriver, Listener, Token};
use crate::{
    timer::{TaskKind, Timer},
    Config, Result,
};

pub struct Server {
    db: Db,
    io: IoDriver,
    last_id: u64,
    listener: Listener,
    connection_timeout: Option<Duration>,
    connections: HashMap<u64, Connection>,
}

impl Server {
    pub fn new(config: Config) -> Result<Server> {
        let db = Db::default();
        let io = IoDriver::new()?;

        let tcp = TcpListener::bind(&config.addr)?;
        let addr = tcp.local_addr()?;
        info!("server is running at {}", addr);
        let mut listener = Listener::new(0, tcp.into_raw_fd(), io.clone());
        listener.prepare_accept()?;

        Ok(Self {
            db,
            io,
            last_id: 0,
            listener,
            connection_timeout: config.connection_timeout,
            connections: HashMap::new(),
        })
    }

    pub fn run(&mut self) -> Result<()> {
        let mut timer = Timer::default();
        self.register_scheduled_tasks(&mut timer);
        loop {
            self.tick(&mut timer)?;
        }
    }

    fn tick(&mut self, timer: &mut Timer) -> Result<()> {
        let mut io = self.io.clone();
        io.wait(1, timer.duration_since_now()).map_err(|err| {
            error!(%err, "wait io");
            err
        })?;

        let mut cq = io.dequeue();
        loop {
            cq.sync();
            if cq.is_empty() {
                break;
            }

            for cqe in &mut cq {
                if cqe.result() == -libc::ETIME {
                    // skip timeout entry
                    continue;
                }

                let id = Token::id(cqe.user_data());
                if id == self.listener.id() {
                    self.handle_listener(cqe)?;
                } else {
                    self.handle_connection(id, cqe)?;
                }
            }
        }

        timer.dispatch_tasks();

        Ok(())
    }

    fn next_id(&mut self) -> u64 {
        self.last_id += 1;
        self.last_id
    }

    fn handle_listener(&mut self, cqe: cqueue::Entry) -> Result<()> {
        let id = self.next_id();
        if let Ok(fd) = self.listener.complete_accept(cqe) {
            let tcp = unsafe { TcpStream::from_raw_fd(fd) };
            tcp.set_nodelay(true).unwrap();
            let fd = tcp.into_raw_fd();
            let conn = Connection::new(id, fd, self.io.clone(), self.db.clone());
            self.connections.insert(id, conn);
        }
        self.listener.prepare_accept()
    }

    fn handle_connection(&mut self, id: u64, cqe: cqueue::Entry) -> Result<()> {
        let conn = self.connections.get_mut(&id).unwrap();
        if conn.tick(cqe) {
            self.connections.remove(&id);
        }
        Ok(())
    }

    fn on_recycle_idle_connections(&mut self) {
        if let Some(timeout) = self.connection_timeout {
            self.connections.retain(|token, connection| {
                if connection.elapsed_from_last_interation() > timeout {
                    trace!(token = token, "closing idle client");
                    false
                } else {
                    true
                }
            });
        }
    }

    fn on_db_tick(&mut self) {}

    fn register_scheduled_tasks(&mut self, timer: &mut Timer) {
        let db_tick_interval = Duration::from_millis(100);
        let conn_idle_interval = Duration::from_millis(500);
        timer.add(
            TaskKind::Interval(db_tick_interval),
            Server::on_db_tick,
            self,
        );
        timer.add(
            TaskKind::Interval(conn_idle_interval),
            Server::on_recycle_idle_connections,
            self,
        );
    }
}
