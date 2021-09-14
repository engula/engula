mod local_manifest;
mod manifest_service;
mod remote_manifest;

pub use local_manifest::LocalManifest;
pub use manifest_server::ManifestServer;
pub use manifest_service::ManifestService;
pub use proto::*;
pub use remote_manifest::RemoteManifest;

use async_trait::async_trait;

use crate::{
    error::Result,
    format::{self, TableDesc},
};

mod proto {
    tonic::include_proto!("engula.manifest");
}

#[derive(Clone, Debug)]
pub struct ManifestOptions {
    pub num_levels: usize,
}

impl ManifestOptions {
    pub fn default() -> ManifestOptions {
        ManifestOptions { num_levels: 4 }
    }
}

#[async_trait]
pub trait Manifest: Send + Sync {
    async fn current(&self, id: u64) -> Result<VersionDesc>;

    async fn add_table(&self, id: u64, desc: TableDesc) -> Result<VersionDesc>;

    async fn next_number(&self) -> Result<u64>;
}
