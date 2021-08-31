#[derive(Clone, Debug)]
pub struct FileMeta {
    pub file_name: String,
    pub file_size: usize,
}

#[derive(Debug)]
pub struct CompactionInput {
    pub levels: Vec<FileMeta>,
    pub output_file_name: String,
}

#[derive(Debug)]
pub struct CompactionOutput {
    pub input: Vec<FileMeta>,
    pub output: FileMeta,
}
