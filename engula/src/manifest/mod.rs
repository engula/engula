mod local_manifest;
mod manifest_service;
mod remote_manifest;

pub use local_manifest::LocalManifest;
pub use manifest_service::ManifestService;
pub use remote_manifest::RemoteManifest;

use async_trait::async_trait;

use crate::{error::Result, format::TableDesc};

tonic::include_proto!("engula.manifest");

pub struct ManifestOptions {
    pub num_levels: u32,
}

impl ManifestOptions {
    pub fn default() -> ManifestOptions {
        ManifestOptions { num_levels: 4 }
    }
}

#[async_trait]
pub trait Manifest: Send + Sync {
    async fn current(&self) -> Result<VersionDesc>;

    async fn add_table(&self, desc: TableDesc) -> Result<VersionDesc>;

    async fn next_number(&self) -> Result<u64>;
}
