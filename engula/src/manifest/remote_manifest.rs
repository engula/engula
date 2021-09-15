use async_trait::async_trait;
use tonic::{transport::Channel, Request};
use tracing::error;

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
        match client.current(request).await {
            Ok(response) => {
                let output = response.into_inner();
                Ok(output.version.unwrap())
            }
            Err(err) => {
                error!("current {}: {}", id, err);
                Err(err.into())
            }
        }
    }

    async fn add_table(&self, id: u64, table: TableDesc) -> Result<VersionDesc> {
        let mut client = self.client.clone();
        let input = AddTableRequest {
            id,
            table: Some(table.clone()),
        };
        let request = Request::new(input);
        match client.add_table(request).await {
            Ok(response) => {
                let output = response.into_inner();
                Ok(output.version.unwrap())
            }
            Err(err) => {
                error!("add table {} {:?}: {}", id, table, err);
                Err(err.into())
            }
        }
    }

    async fn next_number(&self) -> Result<u64> {
        let mut client = self.client.clone();
        let input = NextNumberRequest::default();
        let request = Request::new(input);
        match client.next_number(request).await {
            Ok(response) => {
                let output = response.into_inner();
                Ok(output.number)
            }
            Err(err) => {
                error!("next number: {}", err);
                Err(err.into())
            }
        }
    }
}
