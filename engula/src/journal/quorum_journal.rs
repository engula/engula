use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tonic::transport::Channel;
use tonic::Request;

use super::proto::*;
use crate::error::Result;
use crate::journal::Journal;

type JournalClient = journal_client::JournalClient<Channel>;

pub struct QuorumJournal {
    clients: Mutex<Vec<JournalClient>>,
    timeout: Duration,
}

impl QuorumJournal {
    #[allow(dead_code)]
    pub async fn new(urls: Vec<String>, timeout: Duration) -> Result<QuorumJournal> {
        let mut clients = Vec::new();
        for url in urls {
            let client = JournalClient::connect(url).await?;
            clients.push(client);
        }
        let journal = QuorumJournal {
            clients: Mutex::new(clients),
            timeout,
        };
        Ok(journal)
    }
}

#[async_trait]
impl Journal for QuorumJournal {
    async fn append(&self, data: Vec<u8>) -> Result<()> {
        let input = AppendRequest { data };
        let mut clients = self.clients.lock().await;
        let mut flights = Vec::new();
        for client in clients.iter_mut() {
            let request = Request::new(input.clone());
            flights.push(Box::pin(client.append(request)));
        }
        let quorum = QuorumFuture::new(flights);
        timeout(self.timeout, quorum).await?;
        Ok(())
    }
}

struct QuorumFuture<F> {
    futures: Vec<F>,
}

impl<F: Future + Unpin> QuorumFuture<F> {
    fn new(futures: Vec<F>) -> QuorumFuture<F> {
        QuorumFuture { futures }
    }
}

impl<F: Future + Unpin> Future for QuorumFuture<F> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut count = 0;
        for f in &mut self.futures {
            let future = Pin::new(f);
            if future.poll(cx).is_ready() {
                count += 1;
            }
        }
        if count > self.futures.len() / 2 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
