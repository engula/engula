use tonic::{Request, Response, Status};

use super::{job_server, CompactRequest, CompactResponse};
use crate::format::SstOptions;
use crate::job::{CompactionInput, JobRuntime};

pub struct Service {
    runtime: Box<dyn JobRuntime>,
}

impl Service {
    #[allow(dead_code)]
    pub fn new(runtime: Box<dyn JobRuntime>) -> Service {
        Service { runtime }
    }
}

#[tonic::async_trait]
impl job_server::Job for Service {
    async fn compact(
        &self,
        request: Request<CompactRequest>,
    ) -> Result<Response<CompactResponse>, Status> {
        let input = request.into_inner();
        let input = CompactionInput {
            input_files: input.files,
            options: SstOptions::default(),
            output_file_number: input.output_file_number,
        };
        let output = self.runtime.compact(input).await?;
        Ok(Response::new(CompactResponse {
            files: output.input_files,
            file: Some(output.output_file),
        }))
    }
}

impl From<Service> for job_server::JobServer<Service> {
    fn from(s: Service) -> job_server::JobServer<Service> {
        job_server::JobServer::new(s)
    }
}
