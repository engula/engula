use std::{path::Path, pin::Pin, sync::Arc};

use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use super::{proto::*, JournalOptions, LocalJournal};
use crate::error::Result;

pub struct JournalService {
    options: JournalOptions,
    journal: Arc<LocalJournal>,
}

impl JournalService {
    pub fn new<P: AsRef<Path>>(dirname: P, options: JournalOptions) -> Result<JournalService> {
        let journal = LocalJournal::new(dirname, options.clone())?;
        Ok(JournalService {
            options,
            journal: Arc::new(journal),
        })
    }
}

type TonicResult<T> = std::result::Result<T, Status>;

#[tonic::async_trait]
impl journal_server::Journal for JournalService {
    type AppendStream = Pin<
        Box<dyn Stream<Item = std::result::Result<JournalOutput, Status>> + Send + Sync + 'static>,
    >;

    async fn append(
        &self,
        request: Request<Streaming<JournalRecord>>,
    ) -> TonicResult<Response<Self::AppendStream>> {
        let journal = self.journal.clone();
        let mut buffer = Vec::with_capacity(1024 * 1024);
        let mut stream = request.into_inner().ready_chunks(self.options.chunk_size);
        let output = async_stream::try_stream! {
            while let Some(batch) = stream.next().await {
                buffer.clear();
                let mut sequence = 0;
                for record in batch {
                    let mut record = record?;
                    sequence = record.sequence;
                    buffer.append(&mut record.data);
                }
                journal.append(&buffer).await?;
                yield JournalOutput { sequence };
            }
        };
        Ok(Response::new(Box::pin(output) as Self::AppendStream))
    }
}
