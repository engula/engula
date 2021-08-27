use engula::{Database, LocalJournal, LocalStorage, Options};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options {
        memtable_size: 1024,
    };
    let journal = LocalJournal::new("/tmp/engula", true)?;
    let storage = LocalStorage::new();
    let db = Database::new(options, Box::new(journal), Box::new(storage)).await;
    let key = vec![1, 2];
    let value = vec![3, 4];
    db.put(key.clone(), value.clone()).await?;
    let got_value = db.get(&key).await?;
    assert_eq!(got_value, Some(value));
    let value = vec![5, 6];
    db.put(key.clone(), value.clone()).await?;
    let got_value = db.get(&key).await?;
    assert_eq!(got_value, Some(value));
    Ok(())
}
