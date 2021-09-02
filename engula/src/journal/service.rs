use tonic::{Request, Response, Status};

use super::journal::Journal;
use super::proto::*;

pub struct Service {
    journal: Box<dyn Journal>,
}

impl Service {
    #[allow(dead_code)]
    pub fn new(journal: Box<dyn Journal>) -> Service {
        Service { journal }
    }
}

#[tonic::async_trait]
impl journal_server::Journal for Service {
    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let input = request.into_inner();
        self.journal.append(input.data).await?;
        Ok(Response::new(AppendResponse {}))
    }
}

impl Into<journal_server::JournalServer<Service>> for Service {
    fn into(self) -> journal_server::JournalServer<Service> {
        journal_server::JournalServer::new(self)
    }
}
