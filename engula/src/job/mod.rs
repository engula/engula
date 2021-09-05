mod local_runtime;
mod remote_runtime;
mod runtime;
mod service;

pub use local_runtime::LocalJobRuntime;
pub use remote_runtime::RemoteJobRuntime;
pub use runtime::JobRuntime;
pub use service::Service as JobService;

use crate::format::{FileDesc, SstOptions};

tonic::include_proto!("engula.job");

pub enum JobInput {
    Compaction(CompactionInput),
}

pub enum JobOutput {
    Compaction(CompactionOutput),
}

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
