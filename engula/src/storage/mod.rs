mod local_storage;
mod storage;

pub use local_storage::LocalStorage;
pub use storage::{
    Storage, StorageOptions, StorageVersion, StorageVersionReceiver, StorageVersionRef,
    StorageVersionSender,
};
