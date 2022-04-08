use std::{io, net::TcpListener, os::unix::prelude::IntoRawFd};

use tracing::info;

use crate::uio;

pub struct Driver {
    inner: uio::Driver,
}

impl Driver {
    pub fn new(addr: String) -> io::Result<Driver> {
        let listener = TcpListener::bind(addr)?;
        info!("server is running at {}", listener.local_addr()?);
        let inner = uio::Driver::new(listener.into_raw_fd())?;
        Ok(Self { inner })
    }

    pub fn run(&mut self) -> io::Result<()> {
        loop {
            self.inner.tick()?;
        }
    }
}
