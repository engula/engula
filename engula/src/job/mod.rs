mod local_runtime;
mod remote_runtime;
mod service;

pub use local_runtime::LocalJobRuntime;
pub use remote_runtime::RemoteJobRuntime;
pub use service::Service as JobService;

use crate::error::Result;
use crate::format::{FileDesc, SstOptions};

use async_trait::async_trait;

tonic::include_proto!("engula.job");

#[derive(Debug)]
pub struct CompactionInput {
    pub options: SstOptions,
    pub input_files: Vec<FileDesc>,
    pub output_file_number: u64,
}

#[derive(Debug)]
pub struct CompactionOutput {
    pub input_files: Vec<FileDesc>,
    pub output_file: FileDesc,
}

#[async_trait]
pub trait JobRuntime: Send + Sync {
    async fn compact(&self, input: CompactionInput) -> Result<CompactionOutput>;
}
