use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use clap::Clap;
use engula::{
    Database, Journal, LocalCompaction, LocalFs, LocalJournal, LocalManifest, ManifestOptions,
    Options, QuorumJournal, Result, SstOptions, SstStorage,
};
use tokio::sync::Barrier;
use tracing::info;

#[derive(Clap, Debug)]
pub struct Command {
    // Database options
    #[clap(long)]
    path: String,
    #[clap(long, default_value = "0")]
    num_cores: usize,
    #[clap(long, default_value = "4")]
    num_levels: usize,
    #[clap(long, default_value = "8")]
    block_size_kb: usize,
    #[clap(long, default_value = "1024")]
    memtable_size_mb: usize,
    #[clap(long, default_value = "4")]
    write_batch_size_mb: usize,
    #[clap(long, default_value = "10000")]
    write_channel_size: usize,
    // Component options
    #[clap(long, default_value = "local")]
    journal_kind: String,
    #[clap(long)]
    journal_path: Option<String>,
    #[clap(long)]
    journal_sync: bool,
    // Benchmark options
    #[clap(long)]
    do_get: bool,
    #[clap(long, default_value = "10000")]
    num_tasks: usize,
    #[clap(long, default_value = "1000000")]
    num_entries: usize,
    #[clap(long, default_value = "100")]
    value_size: usize,
}

impl Command {
    pub async fn run(&self) -> Result<()> {
        info!("{:?}", self);
        let db = self.open().await?;
        let db = Arc::new(db);
        self.bench_put(db.clone()).await;
        if self.do_get {
            self.bench_get(db.clone()).await;
        }
        Ok(())
    }

    async fn open(&self) -> Result<Database> {
        let _ = std::fs::remove_dir_all(&self.path);

        let num_cores = if self.num_cores > 0 {
            self.num_cores
        } else {
            num_cpus::get()
        };
        let options = Options {
            num_cores,
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
        info!("{:?}", options);
        info!("{:?}", sst_options);
        info!("{:?}", manifest_options);

        let journal = self.open_journal().await;
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

    async fn open_journal(&self) -> Arc<dyn Journal> {
        let path = self.journal_path.as_ref().unwrap_or(&self.path);
        match self.journal_kind.as_str() {
            "local" => {
                let journal = LocalJournal::new(path, self.journal_sync).unwrap();
                Arc::new(journal)
            }
            "quorum" => {
                let urls = path.split(',').map(|x| x.to_owned()).collect();
                let timeout = Duration::from_secs(1);
                let journal = QuorumJournal::new(urls, timeout).await.unwrap();
                Arc::new(journal)
            }
            _ => panic!("unknown journal kind"),
        }
    }

    async fn bench_get(&self, db: Arc<Database>) {
        let mut tasks = Vec::new();
        let num_entries_per_task = self.num_entries / self.num_tasks;
        let barrier = Arc::new(Barrier::new(self.num_tasks));
        let now = Instant::now();
        for _ in 0..self.num_tasks {
            let db_clone = db.clone();
            let barrier_clone = barrier.clone();
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
        let num_entries_per_task = self.num_entries / self.num_tasks;
        let mut value = Vec::new();
        value.resize(self.value_size, 0);
        let barrier = Arc::new(Barrier::new(self.num_tasks));
        let now = Instant::now();
        for _ in 0..self.num_tasks {
            let db_clone = db.clone();
            let value_clone = value.clone();
            let barrier_clone = barrier.clone();
            let task = tokio::task::spawn(async move {
                barrier_clone.wait().await;
                for i in 0..num_entries_per_task {
                    let key = i.to_be_bytes();
                    db_clone
                        .put(key.to_vec(), value_clone.clone())
                        .await
                        .unwrap();
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
