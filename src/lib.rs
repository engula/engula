mod list;
mod map;
mod object;
mod uint64;

mod collection;
mod database;
mod universe;

pub use list::ListObject;
pub use map::MapObject;
pub use uint64::Uint64Object;

pub use collection::Collection;
pub use database::Database;
pub use universe::Universe;

pub type Error = Box<dyn std::error::Error>;
pub(crate) type Result<T> = std::result::Result<T, Error>;
