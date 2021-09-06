use super::job_client;
use crate::error::Result;
use crate::job::{CompactRequest, CompactionInput, CompactionOutput, JobRuntime};
use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::Request;

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
    async fn compact(&self, input: CompactionInput) -> Result<CompactionOutput> {
        let input = CompactRequest {
            files: input.input_files,
            output_file_number: input.output_file_number,
            options: Some(input.options),
        };
        let request = Request::new(input);
        let mut client = self.client.lock().await;
        let response = client.compact(request).await?;
        let output = response.into_inner();
        Ok(CompactionOutput {
            input_files: output.files,
            output_file: output.file.unwrap(),
        })
    }
}
