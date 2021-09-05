use tonic::{Request, Response, Status};

use super::{job_server, JobRequest, JobResponse, JobRuntime, JobInput};
use crate::format::SstOptions;
use crate::job::JobOutput;

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
impl job_server::JobRuntime for Service {
    async fn spawn_job(
        &self,
        request: Request<JobRequest>,
    ) -> Result<Response<JobResponse>, Status> {
        let input = request.into_inner();
        let input = JobInput{
            input_files: input.files,
            options: SstOptions::default(),
            output_file_number: input.output_file_number,
        };
        let output = match self.runtime.spawn(input).await?{
            JobOutput::Compaction(c) => c,
        };
        Ok(Response::new(JobResponse {
            files: output.input_files,
            file:  output.output_file,
        }))
    }
}

impl From<Service> for job_server::JobServer<Service> {
    fn from(s: Service) -> job_server::JonServer<Service> {
        job_server::JobServer::new(s)
    }
}