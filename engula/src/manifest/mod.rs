mod local_manifest;
mod remote_manifest;
mod service;

pub use local_manifest::LocalManifest;
pub use remote_manifest::RemoteManifest;
pub use service::Service as ManifestService;

use async_trait::async_trait;

use crate::error::Result;
use crate::format::FileDesc;

tonic::include_proto!("engula.manifest");

#[async_trait]
pub trait Manifest: Send + Sync {
    async fn current(&self) -> Result<VersionDesc>;

    async fn next_number(&self) -> Result<u64>;

    async fn install_flush(&self, desc: FileDesc) -> Result<VersionDesc>;
}
