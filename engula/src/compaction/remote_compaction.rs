use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::{transport::Channel, Request};

use super::{compaction_client, CompactionInput, CompactionOutput, CompactionRuntime};
use crate::error::Result;

type CompactionClient = compaction_client::CompactionClient<Channel>;

pub struct RemoteCompaction {
    client: Mutex<CompactionClient>,
}

impl RemoteCompaction {
    #[allow(dead_code)]
    pub async fn new(url: String) -> Result<RemoteCompaction> {
        let client = CompactionClient::connect(url).await?;
        Ok(RemoteCompaction {
            client: Mutex::new(client),
        })
    }
}

#[async_trait]
impl CompactionRuntime for RemoteCompaction {
    async fn compact(&self, input: CompactionInput) -> Result<CompactionOutput> {
        let mut client = self.client.lock().await;
        let request = Request::new(input);
        let response = client.compact(request).await?;
        Ok(response.into_inner())
    }
}
