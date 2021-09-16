use std::{sync::Arc, time::Instant};

use clap::Clap;
use engula::*;
use tokio::sync::Barrier;

use super::config::Config;

#[derive(Clap, Debug)]
pub struct Command {
    #[clap(short, long)]
    config: String,
}

impl Command {
    pub async fn run(&self) -> Result<()> {
        let config = Config::from_file(&self.config).unwrap();
        println!("{:#?}", config);
        let db = config.new_db().await?;
        self.bench_put(db.clone(), &config).await;
        if config.bench_get {
            self.bench_get(db.clone(), &config).await;
        }
        Ok(())
    }

    async fn bench_get(&self, db: Arc<Database>, config: &Config) {
        let mut tasks = Vec::new();
        let num_entries_per_task = config.num_entries / config.num_tasks;
        let barrier = Arc::new(Barrier::new(config.num_tasks));

        let now = Instant::now();
        for _ in 0..config.num_tasks {
            let db = db.clone();
            let barrier = barrier.clone();
            let task = tokio::task::spawn(async move {
                barrier.wait().await;
                for i in 0..num_entries_per_task {
                    let key = i.to_be_bytes();
                    db.get(&key).await.unwrap().unwrap();
                }
            });
            tasks.push(task);
        }
        for task in tasks {
            task.await.unwrap();
        }

        let elapsed = now.elapsed();
        let qps = config.num_entries as f64 / elapsed.as_secs_f64();
        println!("elapsed: {:?}", elapsed);
        println!("qps: {}", qps);
    }

    async fn bench_put(&self, db: Arc<Database>, config: &Config) {
        let mut tasks = Vec::new();
        let num_entries_per_task = config.num_entries / config.num_tasks;
        let barrier = Arc::new(Barrier::new(config.num_tasks));

        let now = Instant::now();
        for _ in 0..config.num_tasks {
            let mut value = Vec::new();
            value.resize(config.value_size, 0);
            let db = db.clone();
            let barrier = barrier.clone();
            let task = tokio::task::spawn(async move {
                barrier.wait().await;
                for i in 0..num_entries_per_task {
                    let key = i.to_be_bytes();
                    db.put(key.to_vec(), value.clone()).await.unwrap();
                }
            });
            tasks.push(task);
        }
        for task in tasks {
            task.await.unwrap();
        }

        let elapsed = now.elapsed();
        let qps = config.num_entries as f64 / elapsed.as_secs_f64();
        println!("elapsed: {:?}", elapsed);
        println!("qps: {}", qps);
    }
}
