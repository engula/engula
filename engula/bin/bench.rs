use std::{sync::Arc, time::Instant};

use clap::Clap;
use engula::{
    Database, LocalCompaction, LocalFs, LocalJournal, LocalManifest, ManifestOptions, Options,
    Result, SstOptions, SstStorage,
};

#[derive(Clap)]
pub struct Command {
    #[clap(long)]
    path: String,
    #[clap(long, default_value = "4")]
    num_tasks: u64,
    #[clap(long, default_value = "1000")]
    num_entries: u64,
    #[clap(long, default_value = "20")]
    key_size: u64,
    #[clap(long, default_value = "200")]
    value_size: u64,
    #[clap(long, default_value = "4")]
    num_levels: usize,
    #[clap(long, default_value = "8")]
    block_size_kb: usize,
    #[clap(long, default_value = "1")]
    memtable_size_mb: usize,
    #[clap(long, default_value = "1")]
    write_batch_size_mb: usize,
    #[clap(long, default_value = "4096")]
    write_channel_size: usize,
}

impl Command {
    pub async fn run(&self) -> Result<()> {
        let db = self.open_db().await?;
        let db = Arc::new(db);
        let num_entries_per_task = self.num_entries / self.num_tasks;
        let now = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..self.num_tasks {
            let db_clone = db.clone();
            let task = tokio::task::spawn(async move {
                for i in 0..num_entries_per_task {
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
        let elapsed = now.elapsed();
        println!("num_tasks: {}", self.num_tasks);
        println!("num_entries: {}", self.num_entries);
        println!("elapsed: {:?}", elapsed);
        println!("qps: {}", self.num_entries as f64 / elapsed.as_secs_f64());
        Ok(())
    }

    async fn open_db(&self) -> Result<Database> {
        let _ = std::fs::remove_dir_all(&self.path);

        let options = Options {
            memtable_size: self.memtable_size_mb * 1024 * 1024,
            write_batch_size: self.write_batch_size_mb * 1024 * 1024,
            write_channel_size: self.write_channel_size,
        };
        let sst_options = SstOptions {
            block_size: self.block_size_kb * 1024,
        };
        let manifest_options = ManifestOptions {
            num_levels: self.num_levels,
        };

        let journal = Arc::new(LocalJournal::new(&self.path, false)?);
        let fs = Arc::new(LocalFs::new(&self.path)?);
        let storage = Arc::new(SstStorage::new(sst_options, fs, None));
        let runtime = Arc::new(LocalCompaction::new(storage.clone()));
        let manifest = Arc::new(LocalManifest::new(
            manifest_options,
            storage.clone(),
            runtime,
        ));

        Database::new(options, journal, storage, manifest).await
    }
}
