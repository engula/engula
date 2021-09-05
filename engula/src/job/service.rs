use tonic::{Request, Response, Status};

use super::{job_server, JobRequest, JobResponse};
use crate::format::SstOptions;
use crate::job::{CompactionInput, JobInput, JobOutput, JobRuntime};

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
    async fn spawn_job(
        &self,
        request: Request<JobRequest>,
    ) -> Result<Response<JobResponse>, Status> {
        let input = request.into_inner();
        let input = JobInput::Compaction(CompactionInput {
            input_files: input.files,
            options: SstOptions::default(),
            output_file_number: input.output_file_number,
        });
        let JobOutput::Compaction(output) = self.runtime.spawn(input).await?;
        Ok(Response::new(JobResponse {
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
