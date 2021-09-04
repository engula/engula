use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::Request;

use super::{
    manifest_client, GetCurrentRequest, InstallFlushRequest, Manifest, NextNumberRequest,
    VersionDesc,
};
use crate::error::Result;
use crate::format::FileDesc;

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
    async fn current(&self) -> Result<VersionDesc> {
        let input = GetCurrentRequest::default();
        let request = Request::new(input);
        let mut client = self.client.lock().await;
        let response = client.get_current(request).await?;
        let output = response.into_inner();
        Ok(output.version.unwrap())
    }

    async fn next_number(&self) -> Result<u64> {
        let input = NextNumberRequest::default();
        let request = Request::new(input);
        let mut client = self.client.lock().await;
        let response = client.next_number(request).await?;
        let output = response.into_inner();
        Ok(output.number)
    }

    async fn install_flush(&self, file: FileDesc) -> Result<VersionDesc> {
        let input = InstallFlushRequest { file: Some(file) };
        let request = Request::new(input);
        let mut client = self.client.lock().await;
        let response = client.install_flush(request).await?;
        let output = response.into_inner();
        Ok(output.version.unwrap())
    }
}
