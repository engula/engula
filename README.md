# ![Engula](https://engula.com/images/logo-wide.png)

[![Gitter](https://badges.gitter.im/engula/contributors.svg)](https://gitter.im/engula/contributors?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

Engula is a storage engine that empowers engineers to build reliable and cost-effective databases with less effort and more confidence.

Engula's design goals are as follows:

- Elastic: unbundles components to leverage elastic resources on the cloud
- Adaptive: optimizes its own data structures according to dynamic workloads
- Extensible: provides flexible APIs and modules to program application logic
- Platform independent: supports flexible deployments on local hosts, on-premise servers, and cloud platforms

Engula is in the demo stage now.
Please check **[the roadmap](https://github.com/engula/engula/issues/1)** for more details.

Welcome to **review [the design](docs/design.md)** and **join [the room](https://gitter.im/engula/contributors)** to discuss with us.
We also offer full-time jobs. For more information, please get in touch with **careers@engula.com**.

## Usage

You can run the example with:

```
cargo run --example hello
```

```rust
use std::sync::Arc;

use engula::*;

#[tokio::main]
async fn main() -> Result<()> {
    let dirname = "/tmp/engula_test/hello";
    let _ = std::fs::remove_dir_all(dirname);

    // Creates a hybrid storage that reads from a sstable storage and
    // writes to a sstable storage and a parquet storage.
    let fs = Arc::new(LocalFs::new(dirname)?);
    let sstable_options = SstableOptions::default();
    let sstable_storage = Arc::new(SstableStorage::new(fs.clone(), sstable_options));
    let parquet_options = ParquetOptions::default();
    let parquet_storage = Arc::new(ParquetStorage::new(fs.clone(), parquet_options));
    let storage = Arc::new(HybridStorage::new(
        sstable_storage.clone(),
        vec![sstable_storage, parquet_storage],
    ));

    let journal_options = JournalOptions::default();
    let journal = Arc::new(LocalJournal::new(dirname, journal_options)?);

    let manifest_options = ManifestOptions::default();
    let runtime = Arc::new(LocalCompaction::new(storage.clone()));
    let manifest = Arc::new(LocalManifest::new(
        manifest_options,
        storage.clone(),
        runtime,
    ));

    let options = Options::default();
    let db = Database::new(options, journal, storage, manifest).await?;
    let db = Arc::new(db);

    // Puts to the database concurrently.
    let num_tasks = 4u64;
    let num_entries = 1024u64;
    let mut tasks = Vec::new();
    for _ in 0..num_tasks {
        let db = db.clone();
        let task = tokio::task::spawn(async move {
            for i in 0..num_entries {
                let v = i.to_be_bytes().to_vec();
                db.put(v.clone(), v.clone()).await.unwrap();
                let got = db.get(&v).await.unwrap();
                assert_eq!(got, Some(v.clone()));
            }
        });
        tasks.push(task);
    }
    for task in tasks {
        task.await?;
    }

    // Verifies the written entries.
    for i in 0..num_entries {
        let v = i.to_be_bytes().to_vec();
        let got = db.get(&v).await.unwrap();
        assert_eq!(got, Some(v.clone()));
    }

    Ok(())
}
```
