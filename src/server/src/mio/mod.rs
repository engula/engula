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

use std::{collections::HashMap, io, io::Read};

use engula_engine::Db;
use mio::{
    event::Event,
    net::{TcpListener, TcpStream},
    Events, Interest, Poll, Token,
};
use tracing::info;

use crate::{Config, Result};

const SERVER: Token = Token(0);

pub struct Server {
    db: Db,
    listener: TcpListener,
    poll: Poll,
    events: Events,
    next_token: Token,
    connections: HashMap<Token, TcpStream>,
}

impl Server {
    pub fn new(config: Config) -> Result<Server> {
        let db = Db::default();
        let addr = config.addr.parse()?;
        let listener = TcpListener::bind(addr)?;
        info!("server is running at {}", listener.local_addr()?);

        let poll = Poll::new()?;
        let events = Events::with_capacity(1024);

        Ok(Self {
            db,
            listener,
            poll,
            events,
            next_token: Token(SERVER.0 + 1),
            connections: HashMap::new(),
        })
    }

    pub fn run(&mut self) -> Result<()> {
        self.poll
            .registry()
            .register(&mut self.listener, SERVER, Interest::READABLE)?;
        loop {
            self.poll.poll(&mut self.events, None)?;
            for event in self.events.iter() {
                match event.token() {
                    SERVER => loop {
                        let (mut connection, address) = match self.listener.accept() {
                            Ok((connection, address)) => (connection, address),
                            Err(ref e) if would_block(e) => {
                                // If we get a `WouldBlock` error we know our
                                // listener has no more incoming connections queued,
                                // so we can return to polling and wait for some
                                // more.
                                break;
                            }
                            Err(e) => {
                                // If it was any other kind of error, something went
                                // wrong and we terminate with an error.
                                return Err(e.into());
                            }
                        };
                        println!("Accepted connection from: {}", address);

                        let token = self.next_token;
                        self.next_token = Token(token.0 + 1);
                        self.poll.registry().register(
                            &mut connection,
                            token,
                            Interest::READABLE,
                        )?;

                        self.connections.insert(token, connection);
                    },
                    token => {
                        // Maybe received an event for a TCP connection.
                        let done = if let Some(connection) = self.connections.get_mut(&token) {
                            handle_connection_event(connection, event)?
                        } else {
                            // Sporadic events happen, we can safely ignore them.
                            false
                        };
                        if done {
                            if let Some(mut connection) = self.connections.remove(&token) {
                                self.poll.registry().deregister(&mut connection)?;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Returns `true` if the connection is done.
fn handle_connection_event(connection: &mut TcpStream, event: &Event) -> io::Result<bool> {
    if event.is_readable() {
        let mut connection_closed = false;
        let mut received_data = vec![0; 4096];
        let mut bytes_read = 0;
        // We can (maybe) read from the connection.
        loop {
            match connection.read(&mut received_data[bytes_read..]) {
                Ok(0) => {
                    // Reading 0 bytes means the other side has closed the
                    // connection or is done writing, then so are we.
                    connection_closed = true;
                    break;
                }
                Ok(n) => {
                    bytes_read += n;
                    if bytes_read == received_data.len() {
                        received_data.resize(received_data.len() + 1024, 0);
                    }
                }
                // Would block "errors" are the OS's way of saying that the
                // connection is not actually ready to perform this I/O operation.
                Err(ref err) if would_block(err) => break,
                Err(ref err) if interrupted(err) => continue,
                // Other errors we'll consider fatal.
                Err(err) => return Err(err),
            }
        }

        if bytes_read != 0 {
            let received_data = &received_data[..bytes_read];
            if let Ok(str_buf) = std::str::from_utf8(received_data) {
                println!("Received data: {}", str_buf.trim_end());
            } else {
                println!("Received (none UTF-8) data: {:?}", received_data);
            }
        }

        if connection_closed {
            println!("Connection closed");
            return Ok(true);
        }
    }

    Ok(false)
}

fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}
