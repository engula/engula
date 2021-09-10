use tonic::{Request, Response, Status};

use super::{journal_server, AppendRequest, AppendResponse, Journal};

pub struct JournalService {
    journal: Box<dyn Journal>,
}

impl JournalService {
    #[allow(dead_code)]
    pub fn new(journal: Box<dyn Journal>) -> JournalService {
        JournalService { journal }
    }
}

#[tonic::async_trait]
impl journal_server::Journal for JournalService {
    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let input = request.into_inner();
        self.journal.append(input.data).await?;
        Ok(Response::new(AppendResponse {}))
    }
}

impl From<JournalService> for journal_server::JournalServer<JournalService> {
    fn from(s: JournalService) -> journal_server::JournalServer<JournalService> {
        journal_server::JournalServer::new(s)
    }
}
