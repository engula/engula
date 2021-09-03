use std::sync::Arc;

use async_trait::async_trait;
use tokio::task;

use super::runtime::JobRuntime;
use super::{CompactionInput, CompactionOutput, JobInput, JobOutput};
use crate::error::Result;
use crate::file_system::FileSystem;
use crate::format::{
    sst_name, FileDesc, Iterator, MergingIterator, SstBuilder, SstReader, TableBuilder, TableReader,
};

pub struct LocalJobRuntime {
    fs: Arc<dyn FileSystem>,
}

impl LocalJobRuntime {
    pub fn new(fs: Arc<dyn FileSystem>) -> LocalJobRuntime {
        LocalJobRuntime { fs }
    }
}

#[async_trait]
impl JobRuntime for LocalJobRuntime {
    async fn spawn(&self, input: JobInput) -> Result<JobOutput> {
        let fs = self.fs.clone();
        let handle = task::spawn(async move {
            match input {
                JobInput::Compaction(c) => {
                    let result = run_compaction(fs, c).await;
                    result.map(JobOutput::Compaction)
                }
            }
        });
        handle.await?
    }
}

async fn run_compaction(
    fs: Arc<dyn FileSystem>,
    input: CompactionInput,
) -> Result<CompactionOutput> {
    let mut children = Vec::new();
    for desc in &input.input_files {
        let file_name = sst_name(desc.file_number);
        let reader = fs.new_random_access_reader(&file_name).await?;
        let sst_reader = SstReader::open(reader, desc.file_size).await?;
        let iter = sst_reader.new_iterator().await?;
        children.push(iter);
    }
    let mut iter = MergingIterator::new(children);

    let file_name = sst_name(input.output_file_number);
    let file = fs.new_sequential_writer(&file_name).await?;
    let mut builder = SstBuilder::new(input.options, file);

    iter.seek_to_first().await;
    while let Some(v) = iter.current() {
        builder.add(v.0, v.1, v.2).await;
        iter.next().await;
    }
    let file_size = builder.finish().await?;
    let file_desc = FileDesc {
        file_number: input.output_file_number,
        file_size: file_size as u64,
    };
    let output = CompactionOutput {
        input_files: input.input_files,
        output_file: file_desc,
    };
    Ok(output)
}
