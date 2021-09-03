mod local_runtime;
mod runtime;

pub use local_runtime::LocalJobRuntime;
pub use runtime::JobRuntime;

use crate::format::{FileDesc, SstOptions};

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
