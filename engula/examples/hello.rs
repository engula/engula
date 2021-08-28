use engula::{Database, LocalFileSystem, LocalJournal, LocalStorage, Options};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options {
        memtable_size: 1024,
    };
    let dirname = "/tmp/engula";
    let fs = LocalFileSystem::new(dirname)?;
    let storage = LocalStorage::new(Box::new(fs))?;
    let journal = LocalJournal::new(dirname, false)?;
    let db = Database::new(options, Box::new(journal), Box::new(storage)).await;

    let key = vec![1, 2, 3];
    let value = vec![4, 5, 6];
    db.put(key.clone(), value.clone()).await?;
    let got_value = db.get(&key).await?;
    assert_eq!(got_value, Some(value));

    Ok(())
}
