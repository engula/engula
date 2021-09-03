mod block;
mod sstable;
mod table;

mod iterator;
mod merging_iterator;
mod two_level_iterator;

pub use block::{BlockBuilder, BlockHandle};
pub use sstable::{SstBuilder, SstOptions, SstReader};
pub use table::{TableBuilder, TableReader};

pub use iterator::{Iterator, Version};
pub use merging_iterator::MergingIterator;
pub use two_level_iterator::TwoLevelIterator;

tonic::include_proto!("engula.format.v1");

pub type Timestamp = u64;

pub fn sst_name(file_number: u64) -> String {
    format!("{}.sst", file_number)
}
