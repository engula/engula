use engula::{Database, LocalJournal, LocalStorage, Options};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options {
        memtable_size: 1024,
    };
    let journal = LocalJournal::new("/tmp/engula", true)?;
    let storage = LocalStorage::new();
    let db = Database::new(options, Box::new(journal), Box::new(storage));
    let key = "helo".as_bytes().to_owned();
    let value = "world".as_bytes().to_owned();
    db.put(key.clone(), value.clone()).await?;
    let _ = db.get(&key).await?;
    Ok(())
}
