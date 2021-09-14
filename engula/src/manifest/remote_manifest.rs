use async_trait::async_trait;
use tonic::{transport::Channel, Request};

use super::{proto::*, Manifest};
use crate::{error::Result, format::TableDesc};

type ManifestClient = manifest_client::ManifestClient<Channel>;

pub struct RemoteManifest {
    client: ManifestClient,
}

impl RemoteManifest {
    pub async fn new(url: &str) -> Result<RemoteManifest> {
        let client = ManifestClient::connect(url.to_owned()).await?;
        Ok(RemoteManifest { client })
    }
}

#[async_trait]
impl Manifest for RemoteManifest {
    async fn current(&self, id: u64) -> Result<VersionDesc> {
        let mut client = self.client.clone();
        let input = CurrentRequest { id };
        let request = Request::new(input);
        let response = client.current(request).await?;
        let output = response.into_inner();
        Ok(output.version.unwrap())
    }

    async fn add_table(&self, id: u64, table: TableDesc) -> Result<VersionDesc> {
        let mut client = self.client.clone();
        let input = AddTableRequest {
            id,
            table: Some(table),
        };
        let request = Request::new(input);
        let response = client.add_table(request).await?;
        let output = response.into_inner();
        Ok(output.version.unwrap())
    }

    async fn next_number(&self) -> Result<u64> {
        let mut client = self.client.clone();
        let input = NextNumberRequest::default();
        let request = Request::new(input);
        let response = client.next_number(request).await?;
        let output = response.into_inner();
        Ok(output.number)
    }
}
