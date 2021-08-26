mod database;
mod entry;
mod journal;
mod memtable;
mod storage;

pub use database::Database;
pub use journal::{Journal, LocalJournal};
pub use storage::{MemStorage, Storage};

pub type Error = Box<dyn std::error::Error>;
pub(crate) type Result<T> = std::result::Result<T, Error>;
