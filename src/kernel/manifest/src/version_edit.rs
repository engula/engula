/// A batch of modifications to a Version.
pub struct VersionEdit {}

impl VersionEdit {
    /// Adds a file to the collection.
    pub fn add_file(&mut self, _co_id: u64, _fd: FileDescriptor) {}

    /// Removes a file from the collection.
    pub fn remove_file(&mut self, _co_id: u64, _file_id: u64) {}
}

pub struct FileDescriptor {}
