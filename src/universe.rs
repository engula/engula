use crate::database::Database;
use crate::Result;

pub struct Universe {}

impl Universe {
    pub fn open(url: &str) -> Result<Universe> {
        Ok(Universe {})
    }

    pub fn create_database(&self, name: &str) -> Result<Database> {
        Ok(Database {})
    }
}
