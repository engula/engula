use std::sync::Arc;

use engula::{
    Database, FileSystem, JobRuntime, LocalFileSystem, LocalJobRuntime, LocalJournal, LocalStorage,
    Options, StorageOptions,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::default();
    let storage_options = StorageOptions::default();
    let dirname = "/tmp/engula";
    let _ = std::fs::remove_dir_all(dirname);
    let fs = LocalFileSystem::new(dirname)?;
    let fs: Arc<dyn FileSystem> = Arc::new(fs);
    let job = LocalJobRuntime::new(fs.clone());
    let job: Arc<dyn JobRuntime> = Arc::new(job);
    let storage = LocalStorage::new(storage_options, fs, job);
    let journal = LocalJournal::new(dirname, false)?;
    let db = Database::new(options, Arc::new(journal), Arc::new(storage)).await;
    let db = Arc::new(db);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let db2 = db.clone();
        let task = tokio::task::spawn(async move {
            for i in 0..1024u64 {
                let v = i.to_be_bytes().to_vec();
                db2.put(v.clone(), v.clone()).await.unwrap();
                let got = db2.get(&v).await.unwrap();
                assert_eq!(got, Some(v.clone()));
            }
        });
        tasks.push(task);
    }
    for task in tasks {
        task.await?;
    }
    // Check data again
    for i in 0..1024u64 {
        let v = i.to_be_bytes().to_vec();
        let got = db.get(&v).await.unwrap();
        assert_eq!(got, Some(v.clone()));
    }
    Ok(())
}
