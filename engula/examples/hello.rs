use std::sync::Arc;

use engula::{
    Database, LocalCompaction, LocalFs, LocalJournal, LocalManifest, ManifestOptions, Options,
    SstOptions, SstStorage,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dirname = "/tmp/engula";
    let _ = std::fs::remove_dir_all(dirname);

    let options = Options::default();
    let sst_options = SstOptions::default();
    let manifest_options = ManifestOptions::default();

    let journal = Arc::new(LocalJournal::new(dirname, false)?);
    let fs = Arc::new(LocalFs::new(dirname)?);
    let storage = Arc::new(SstStorage::new(sst_options, fs, None));
    let runtime = Arc::new(LocalCompaction::new(storage.clone()));
    let manifest = Arc::new(LocalManifest::new(
        manifest_options,
        storage.clone(),
        runtime,
    ));

    let db = Database::new(options, journal, storage, manifest)
        .await
        .unwrap();
    let db = Arc::new(db);
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let db_clone = db.clone();
        let task = tokio::task::spawn(async move {
            for i in 0..1024u64 {
                let v = i.to_be_bytes().to_vec();
                db_clone.put(v.clone(), v.clone()).await.unwrap();
                let got = db_clone.get(&v).await.unwrap();
                assert_eq!(got, Some(v.clone()));
            }
        });
        tasks.push(task);
    }
    for task in tasks {
        task.await?;
    }
    for i in 0..1024u64 {
        let v = i.to_be_bytes().to_vec();
        let got = db.get(&v).await.unwrap();
        assert_eq!(got, Some(v.clone()));
    }
    Ok(())
}
