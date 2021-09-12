use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::{transport::Channel, Request};

use super::{
    manifest_client, AddTableRequest, CurrentRequest, Manifest, NextNumberRequest, VersionDesc,
};
use crate::{error::Result, format::TableDesc};

type ManifestClient = manifest_client::ManifestClient<Channel>;

pub struct RemoteManifest {
    client: Mutex<ManifestClient>,
}

impl RemoteManifest {
    #[allow(dead_code)]
    pub async fn new(url: String) -> Result<RemoteManifest> {
        let client = ManifestClient::connect(url).await?;
        Ok(RemoteManifest {
            client: Mutex::new(client),
        })
    }
}

#[async_trait]
impl Manifest for RemoteManifest {
    async fn current(&self, id: u64) -> Result<VersionDesc> {
        let mut client = self.client.lock().await;
        let input = CurrentRequest { id };
        let request = Request::new(input);
        let response = client.current(request).await?;
        let output = response.into_inner();
        Ok(output.version.unwrap())
    }

    async fn add_table(&self, id: u64, table: TableDesc) -> Result<VersionDesc> {
        let mut client = self.client.lock().await;
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
        let mut client = self.client.lock().await;
        let input = NextNumberRequest::default();
        let request = Request::new(input);
        let response = client.next_number(request).await?;
        let output = response.into_inner();
        Ok(output.number)
    }
}
