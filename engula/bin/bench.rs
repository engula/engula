use std::{sync::Arc, time::Instant};

use clap::Clap;
use engula::*;
use tokio::sync::Barrier;

#[derive(Clap, Debug)]
pub struct Command {
    // Database options
    #[clap(long, default_value = "8")]
    num_cores: usize,
    #[clap(long, default_value = "4")]
    num_levels: usize,
    #[clap(long, default_value = "8")]
    block_size_kb: usize,
    #[clap(long, default_value = "1024")]
    memtable_size_mb: usize,
    #[clap(long, default_value = "1024")]
    write_channel_size: usize,
    // Component options
    #[clap(long, default_value = "local")]
    journal_kind: String,
    #[clap(long)]
    journal_path: String,
    #[clap(long)]
    journal_sync: bool,
    #[clap(long)]
    storage_path: String,
    // Benchmark options
    #[clap(long)]
    do_get: bool,
    #[clap(long, default_value = "100000")]
    num_tasks: usize,
    #[clap(long, default_value = "1000000")]
    num_entries: usize,
    #[clap(long, default_value = "100")]
    value_size: usize,
}

impl Command {
    pub async fn run(&self) -> Result<()> {
        println!("{:#?}", self);
        let db = self.open().await?;
        let db = Arc::new(db);
        self.bench_put(db.clone()).await;
        if self.do_get {
            self.bench_get(db.clone()).await;
        }
        Ok(())
    }

    async fn open(&self) -> Result<Database> {
        let options = Options {
            num_cores: self.num_cores,
            memtable_size: self.memtable_size_mb * 1024 * 1024,
            write_channel_size: self.write_channel_size,
        };
        let manifest_options = ManifestOptions {
            num_levels: self.num_levels,
        };

        let journal = self.open_journal().await;
        let storage = self.open_storage().await;
        let runtime = Arc::new(LocalCompaction::new(storage.clone()));
        let manifest = Arc::new(LocalManifest::new(
            manifest_options,
            storage.clone(),
            runtime,
        ));

        Database::new(options, journal, storage, manifest).await
    }

    async fn open_journal(&self) -> Arc<dyn Journal> {
        let options = JournalOptions {
            sync: self.journal_sync,
            chunk_size: self.write_channel_size,
        };
        match self.journal_kind.as_str() {
            "local" => {
                let _ = std::fs::remove_dir_all(&self.journal_path);
                let journal = LocalJournal::new(&self.journal_path, options).unwrap();
                Arc::new(journal)
            }
            "quorum" => {
                let urls = self.journal_path.split(',').map(|x| x.to_owned()).collect();
                let journal = QuorumJournal::new(urls, options).await.unwrap();
                Arc::new(journal)
            }
            _ => panic!("unknown journal kind"),
        }
    }

    async fn open_storage(&self) -> Arc<dyn Storage> {
        let options = SstOptions {
            block_size: self.block_size_kb * 1024,
        };
        let _ = std::fs::remove_dir_all(&self.storage_path);
        let fs = Arc::new(LocalFs::new(&self.storage_path).unwrap());
        let storage = SstStorage::new(options, fs, None);
        Arc::new(storage)
    }

    async fn bench_get(&self, db: Arc<Database>) {
        let mut tasks = Vec::new();
        let barrier = Arc::new(Barrier::new(self.num_tasks));

        let now = Instant::now();
        for _ in 0..self.num_tasks {
            let db_clone = db.clone();
            let barrier_clone = barrier.clone();
            let num_entries_per_task = self.num_entries / self.num_tasks;
            let task = tokio::task::spawn(async move {
                barrier_clone.wait().await;
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
        let mut tasks = Vec::new();
        let barrier = Arc::new(Barrier::new(self.num_tasks));

        let now = Instant::now();
        for _ in 0..self.num_tasks {
            let mut value = Vec::new();
            value.resize(self.value_size, 0);
            let db_clone = db.clone();
            let barrier_clone = barrier.clone();
            let num_entries_per_task = self.num_entries / self.num_tasks;
            let task = tokio::task::spawn(async move {
                barrier_clone.wait().await;
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
