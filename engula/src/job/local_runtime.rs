use std::sync::Arc;

use async_trait::async_trait;
use tokio::task;

use super::compaction::{CompactionInput, CompactionOutput, FileMeta};
use super::job::{JobInput, JobOutput};
use super::runtime::JobRuntime;
use crate::error::Result;
use crate::file_system::FileSystem;
use crate::format::{
    Iterator, MergingIterator, SstBuilder, SstOptions, SstReader, TableBuilder, TableReader,
};

pub struct LocalJobRuntime {
    fs: Arc<Box<dyn FileSystem>>,
}

impl LocalJobRuntime {
    pub fn new(fs: Arc<Box<dyn FileSystem>>) -> LocalJobRuntime {
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
                    result.map(|x| JobOutput::Compaction(x))
                }
            }
        });
        handle.await?
    }
}

async fn run_compaction(
    fs: Arc<Box<dyn FileSystem>>,
    input: CompactionInput,
) -> Result<CompactionOutput> {
    let mut children = Vec::new();
    for level in &input.levels {
        let file = fs.new_random_access_reader(&level.file_name).await?;
        let reader = SstReader::open(file, level.file_size).await?;
        let iter = reader.new_iterator().await?;
        children.push(iter);
    }
    let mut iter = MergingIterator::new(children);

    let options = SstOptions::default();
    let file = fs.new_sequential_writer(&input.output_file_name).await?;
    let mut builder = SstBuilder::new(options, file);

    iter.seek_to_first().await;
    while let Some(v) = iter.current() {
        builder.add(v.0, v.1, v.2).await;
        iter.next().await;
    }
    let file_size = builder.finish().await?;
    let file_meta = FileMeta {
        file_name: input.output_file_name.clone(),
        file_size,
    };
    let output = CompactionOutput {
        input: input.levels,
        output: file_meta,
    };
    Ok(output)
}
