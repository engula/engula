pub struct StorageError {}

pub type StorageResult<T> = Result<T, StorageError>;
