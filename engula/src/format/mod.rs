mod block;
mod cache;
mod filter;
mod parquet_builder;
mod sstable;
mod table;

mod bloom_filter;
mod iterator;
mod merging_iterator;
mod two_level_iterator;

pub use block::{BlockBuilder, BlockHandle};
pub use cache::Cache;
pub use filter::{FilterBuilder, FilterReader};
pub use parquet_builder::ParquetBuilder;
pub use sstable::{SstBuilder, SstReader};
pub use table::{TableBuilder, TableReader};

pub use iterator::{Entry, Iterator};
pub use merging_iterator::MergingIterator;
pub use two_level_iterator::TwoLevelIterator;

tonic::include_proto!("engula.format");

pub type Timestamp = u64;

pub fn sst_name(file_number: u64) -> String {
    format!("{}.sst", file_number)
}
