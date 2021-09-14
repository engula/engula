use tonic::{Request, Response, Status};

use super::{proto::*, CompactionRuntime};

pub struct CompactionService {
    runtime: Box<dyn CompactionRuntime>,
}

impl CompactionService {
    pub fn new(runtime: Box<dyn CompactionRuntime>) -> CompactionService {
        CompactionService { runtime }
    }
}

#[tonic::async_trait]
impl compaction_server::Compaction for CompactionService {
    async fn compact(
        &self,
        request: Request<CompactionInput>,
    ) -> Result<Response<CompactionOutput>, Status> {
        let input = request.into_inner();
        let output = self.runtime.compact(input).await?;
        Ok(Response::new(output))
    }
}
