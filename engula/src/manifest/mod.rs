mod local_manifest;

pub use local_manifest::LocalManifest;

use async_trait::async_trait;

use crate::error::Result;
use crate::format::FileDesc;

tonic::include_proto!("engula.manifest");

#[async_trait]
pub trait Manifest: Send + Sync {
    async fn current(&self) -> Result<VersionDesc>;

    async fn next_file_number(&self) -> Result<u64>;

    async fn install_flush_output(&self, desc: FileDesc) -> Result<VersionDesc>;
}
