mod mem_bucket;
mod mem_object;
mod mem_storage;

pub use self::mem_storage::MemStorage;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    #[tokio::test]
    async fn mem_storage() -> Result<()> {
        let storage = MemStorage::default();
        let _ = storage.create_bucket("a").await?;
        Ok(())
    }
}
