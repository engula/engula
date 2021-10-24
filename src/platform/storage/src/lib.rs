mod bucket_handle;
mod error;
mod object_handle;
mod object_storage;

pub use self::{
    bucket_handle::BucketHandle,
    error::{StorageError, StorageResult},
    object_handle::ObjectHandle,
    object_storage::ObjectStorage,
};
