mod common;
mod database;
mod file_system;
mod format;
mod journal;
mod memtable;
mod storage;

pub use database::{Database, Options};
pub use file_system::{FileSystem, LocalFileSystem};
pub use journal::{Journal, LocalJournal};
pub use storage::{LocalStorage, Storage};

pub type Error = Box<dyn std::error::Error>;
pub(crate) type Result<T> = std::result::Result<T, Error>;
