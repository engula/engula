use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use futures::StreamExt;
use tokio::{
    sync::{mpsc, Mutex},
    task,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, Request};

use super::{proto::*, write::WriteBatch, Journal, JournalOptions};
use crate::error::Result;

type JournalClient = journal_client::JournalClient<Channel>;

pub struct QuorumJournal {
    options: JournalOptions,
    clients: Vec<JournalClient>,
}

impl QuorumJournal {
    pub async fn new(urls: Vec<String>, options: JournalOptions) -> Result<QuorumJournal> {
        let mut clients = Vec::new();
        for url in urls {
            let client = JournalClient::connect(url).await?;
            clients.push(client);
        }
        Ok(QuorumJournal { options, clients })
    }
}

#[async_trait]
impl Journal for QuorumJournal {
    async fn append_stream(&self, rx: mpsc::Receiver<WriteBatch>) -> Result<()> {
        let producer = Arc::new(Mutex::new(VecDeque::new()));
        let consumer = producer.clone();
        let mut stream = ReceiverStream::new(rx).ready_chunks(self.options.chunk_size);
        let input_stream = async_stream::stream! {
            let mut sequence = 0;
            while let Some(mut batches) = stream.next().await {
                sequence += 1;
                let mut data = Vec::with_capacity(1024 * 1024);
                for batch in &mut batches {
                    data.append(&mut batch.buffer);
                }
                producer.lock().await.push_back((sequence, batches));
                yield JournalRecord { sequence, data };
            }
        };

        // TODO: write to the quorum.
        let mut client = self.clients[0].clone();
        let request = Request::new(input_stream);
        let response = client.append(request).await?;
        let mut output_stream = response.into_inner();

        while let Some(output) = output_stream.message().await? {
            let readies = {
                let mut consumer = consumer.lock().await;
                let index = consumer.partition_point(|x| x.0 <= output.sequence);
                consumer.drain(0..index).collect::<VecDeque<_>>()
            };
            for (_, batches) in readies {
                for batch in batches {
                    batch.tx.send(batch.writes).await?;
                }
            }
            task::yield_now().await;
        }
        Ok(())
    }
}
