mod compaction;
mod local_runtime;
mod runtime;

pub use compaction::{CompactionInput, CompactionOutput, FileMeta};
pub use local_runtime::LocalJobRuntime;
pub use runtime::JobRuntime;

pub enum JobInput {
    Compaction(CompactionInput),
}

pub enum JobOutput {
    Compaction(CompactionOutput),
}
