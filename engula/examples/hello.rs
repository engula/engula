use engula::{Database, MemJournal, MemStorage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let journal = MemJournal::new();
    let storage = MemStorage::new();
    let db = Database::new(journal, storage);
    let key = "helo".as_bytes().to_owned();
    let value = "world".as_bytes().to_owned();
    db.put(key.clone(), value.clone()).await?;
    let _ = db.get(&key).await?;
    Ok(())
}
