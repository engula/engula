use async_trait::async_trait;

use crate::{error::Result, version_edit::VersionEdit};

/// A manifest that records the layout and metadata of files.
#[async_trait]
pub trait Manifest {
    async fn commit(edit: VersionEdit) -> Result<()>;
}
