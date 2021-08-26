use std::path::Path;

use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use super::journal::Journal;
use crate::Result;

pub struct LocalJournal {
    file: Mutex<File>,
    sync: bool,
}

impl LocalJournal {
    pub fn new<P: AsRef<Path>>(dirname: P, sync: bool) -> Result<LocalJournal> {
        let filename = dirname.as_ref().join("engula.log");
        std::fs::create_dir_all(dirname)?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(filename)?;
        Ok(LocalJournal {
            file: Mutex::new(File::from_std(file)),
            sync,
        })
    }
}

#[async_trait]
impl Journal for LocalJournal {
    async fn append(&self, data: Vec<u8>) -> Result<()> {
        let mut file = self.file.lock().await;
        file.write_all(&data).await?;
        if self.sync {
            file.sync_data().await?;
        }
        Ok(())
    }
}
