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

    for i in 0..1024u64 {
        let v = i.to_be_bytes().to_vec();
        db.put(v.clone(), v.clone()).await?;
        let got = db.get(&v).await?;
        assert_eq!(got, Some(v.clone()));
    }

    Ok(())
}
