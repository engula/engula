use crate::collection::Collection;
use crate::Result;

pub struct Database {}

impl Database {
    pub fn create_collection(&self, name: &str) -> Result<Collection> {
        Ok(Collection {})
    }
}
