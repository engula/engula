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

use std::{collections::HashMap, time::Duration};

use engula_engine::Db;
use mio::{net::TcpListener, Events, Interest, Poll, Token};
use tracing::{info, trace};

use super::would_block;
use crate::{mio::connection::Connection, Config, Error, Result};

const SERVER: Token = Token(0);

pub struct Server {
    db: Db,
    next: Token,
    listener: TcpListener,
    connection_timeout: Option<Duration>,
    connections: HashMap<Token, Connection>,
}

impl Server {
    pub fn new(config: Config) -> Result<Server> {
        let addr = config.addr.parse()?;
        let listener = TcpListener::bind(addr)?;

        info!("server is running at {}", listener.local_addr()?);

        Ok(Self {
            db: Db::default(),
            next: Token(SERVER.0 + 1),
            listener,
            connection_timeout: config.connection_timeout,
            connections: HashMap::new(),
        })
    }

    pub fn run(&mut self) -> Result<()> {
        let tick = Duration::from_millis(500);
        let mut poll = Poll::new()?;
        let mut events = Events::with_capacity(128);
        poll.registry()
            .register(&mut self.listener, SERVER, Interest::READABLE)?;
        loop {
            poll.poll(&mut events, Some(tick))?;
            for event in events.iter() {
                match event.token() {
                    SERVER => loop {
                        let (mut stream, address) = match self.listener.accept() {
                            Ok((stream, address)) => (stream, address),
                            Err(ref e) if would_block(e) => break,
                            Err(e) => return Err(Error::Io(e)),
                        };
                        trace!("Accepted connection from: {}", address);
                        let token = self.token();
                        let interest = Interest::READABLE | Interest::WRITABLE;
                        poll.registry().register(&mut stream, token, interest)?;
                        stream.set_nodelay(true)?;
                        let connection = Connection::new(self.db.clone(), stream);
                        self.connections.insert(token, connection);
                    },
                    token => {
                        trace!("receive event {:?} for token {:?}", event, token);
                        let done = if let Some(connection) = self.connections.get_mut(&token) {
                            connection.handle_connection_event(event)?
                        } else {
                            // Sporadic events happen, we can safely ignore them.
                            false
                        };
                        if done {
                            if let Some(mut connection) = self.connections.remove(&token) {
                                poll.registry().deregister(connection.stream())?;
                            }
                        }
                    }
                }
            }

            if let Some(timeout) = self.connection_timeout {
                self.connections.retain(|token, connection| {
                    if connection.elapsed_from_last_interation() > timeout {
                        trace!(token = token.0, "closing idle client");
                        poll.registry().deregister(connection.stream()).unwrap();
                        false
                    } else {
                        true
                    }
                });
            }
        }
    }

    fn token(&mut self) -> Token {
        let token = self.next;
        self.next = Token(token.0 + 1);
        token
    }
}
