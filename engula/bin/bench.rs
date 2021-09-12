use std::{sync::Arc, time::Instant};

use clap::Clap;
use engula::{
    Database, LocalCompaction, LocalFs, LocalJournal, LocalManifest, ManifestOptions, Options,
    Result, SstOptions, SstStorage,
};

#[derive(Clap)]
pub struct Command {
    // Benchmark options
    #[clap(long)]
    get: bool,
    #[clap(long, default_value = "1000")]
    num_tasks: usize,
    #[clap(long, default_value = "1000000")]
    num_entries: usize,
    #[clap(long, default_value = "100")]
    value_size: usize,
    // Database options
    #[clap(long)]
    path: String,
    #[clap(long)]
    sync: bool,
    #[clap(long, default_value = "4")]
    num_cores: usize,
    #[clap(long, default_value = "4")]
    num_levels: usize,
    #[clap(long, default_value = "8")]
    block_size_kb: usize,
    #[clap(long, default_value = "1")]
    memtable_size_mb: usize,
    #[clap(long, default_value = "1")]
    write_batch_size_mb: usize,
    #[clap(long, default_value = "10000")]
    write_channel_size: usize,
}

impl Command {
    pub async fn run(&self) -> Result<()> {
        let db = self.open().await?;
        let db = Arc::new(db);
        self.bench_put(db.clone()).await;
        if self.get {
            self.bench_get(db.clone()).await;
        }
        Ok(())
    }

    async fn open(&self) -> Result<Database> {
        let _ = std::fs::remove_dir_all(&self.path);

        let options = Options {
            num_cores: self.num_cores,
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

        let journal = Arc::new(LocalJournal::new(&self.path, self.sync)?);
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

    async fn bench_get(&self, db: Arc<Database>) {
        let now = Instant::now();
        let mut tasks = Vec::new();
        let num_entries_per_task = self.num_entries / self.num_tasks;
        for _ in 0..self.num_tasks {
            let db_clone = db.clone();
            let task = tokio::task::spawn(async move {
                for i in 0..num_entries_per_task {
                    let key = i.to_be_bytes();
                    db_clone.get(&key).await.unwrap().unwrap();
                }
            });
            tasks.push(task);
        }
        for task in tasks {
            task.await.unwrap();
        }
        let elapsed = now.elapsed();
        println!("elapsed: {:?}", elapsed);
        println!("qps: {}", self.num_entries as f64 / elapsed.as_secs_f64());
    }

    async fn bench_put(&self, db: Arc<Database>) {
        let now = Instant::now();
        let mut tasks = Vec::new();
        let num_entries_per_task = self.num_entries / self.num_tasks;
        for _ in 0..self.num_tasks {
            let db_clone = db.clone();
            let mut value = Vec::new();
            value.resize(self.value_size, 0);
            let task = tokio::task::spawn(async move {
                for i in 0..num_entries_per_task {
                    let key = i.to_be_bytes();
                    db_clone.put(key.to_vec(), value.clone()).await.unwrap();
                }
            });
            tasks.push(task);
        }
        for task in tasks {
            task.await.unwrap();
        }
        let elapsed = now.elapsed();
        println!("elapsed: {:?}", elapsed);
        println!("qps: {}", self.num_entries as f64 / elapsed.as_secs_f64());
    }
}
