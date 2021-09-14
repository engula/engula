use async_trait::async_trait;
use tonic::{transport::Channel, Request};

use super::{proto::*, CompactionRuntime};
use crate::error::Result;

type CompactionClient = compaction_client::CompactionClient<Channel>;

pub struct RemoteCompaction {
    client: CompactionClient,
}

impl RemoteCompaction {
    pub async fn new(url: &str) -> Result<RemoteCompaction> {
        let client = CompactionClient::connect(url.to_owned()).await?;
        Ok(RemoteCompaction { client })
    }
}

#[async_trait]
impl CompactionRuntime for RemoteCompaction {
    async fn compact(&self, input: CompactionInput) -> Result<CompactionOutput> {
        let mut client = self.client.clone();
        let request = Request::new(input);
        let response = client.compact(request).await?;
        Ok(response.into_inner())
    }
}
