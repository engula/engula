# ![Engula](https://engula.com/images/logo-wide.png)

[![Gitter](https://badges.gitter.im/engula/contributors.svg)](https://gitter.im/engula/contributors?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

Engula is a cloud-native storage engine that empowers engineers to build next-generation data infrastructures with less effort and more confidence.

Engula is in the demo stage now.
Check **[the roadmap](https://github.com/engula/engula/issues/1)** for more details.

Welcome to **review [the design](docs/design.md)** and **join [the room](https://gitter.im/engula/contributors)** to discuss with us.
We also offer full-time jobs.
Please get in touch with **careers@engula.com** for more information.

## Usage

```rust
use std::sync::Arc;

use engula::{
    Database, FileSystem, JobRuntime, LocalFileSystem, LocalJobRuntime, LocalJournal, LocalStorage,
    Options, StorageOptions,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options {
        memtable_size: 1024,
    };
    let storage_options = StorageOptions::default();
    let dirname = "/tmp/engula";
    let fs = LocalFileSystem::new(dirname)?;
    let fs: Arc<Box<dyn FileSystem>> = Arc::new(Box::new(fs));
    let job = LocalJobRuntime::new(fs.clone());
    let job: Arc<Box<dyn JobRuntime>> = Arc::new(Box::new(job));
    let storage = LocalStorage::new(storage_options, fs, job)?;
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
```