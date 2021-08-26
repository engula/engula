use async_trait::async_trait;

use crate::Result;

#[async_trait]
pub trait Journal: Send + Sync {
    async fn append(&self, data: Vec<u8>) -> Result<()>;
}

pub struct MemJournal {}

impl MemJournal {
    pub fn new() -> MemJournal {
        MemJournal {}
    }
}

#[async_trait]
impl Journal for MemJournal {
    async fn append(&self, data: Vec<u8>) -> Result<()> {
        Ok(())
    }
}
