
use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::Request;

use super::{job_client, JobRequest, Job};
use crate::error::Result;
use crate::JobRuntime;
use crate::job::{JobInput, JobOutput, CompactionOutput};


type JobClient = job_client::JobClient<Channel>;

pub struct RemoteJobRuntime {
    client: Mutex<JobClient>,
}

impl RemoteJobRuntime {
    #[allow(dead_code)]
    pub async fn new(url: String) -> Result<RemoteJobRuntime> {
       let client = JobClient::connect(url).await?;
        Ok(RemoteJobRuntime {
            client: Mutex::new(client),
        })
    }
}

#[async_trait]
impl JobRuntime for RemoteJobRuntime {
    async fn spawn(&self, input: JobInput) -> Result<JobOutput()> {
        let input = match input {
            JobInput::Compaction(c) => c,
        };
        let files = input.input_files;
        let output_file_number = input.output_file_number;
        let input = JobRequest{
            files,
            output_file_number
        };
        let request = Request::new(input);
        let mut client = self.client.lock().await;
        let response = client.spawn_job(request).await?;
        let output = response.into_inner();
        Ok(JobOutput::Compaction(CompactionOutput{
            input_files: output.files.unwarp(),
            output_file: output.file.unwarp(),
        }))
    }
}
