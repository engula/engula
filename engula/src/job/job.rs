use super::compaction::{CompactionInput, CompactionOutput};

pub enum JobInput {
    Compaction(CompactionInput),
}

pub enum JobOutput {
    Compaction(CompactionOutput),
}
