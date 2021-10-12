# ![Engula](https://engula.com/images/logo-wide.png)

[![Gitter](https://badges.gitter.im/engula/contributors.svg)](https://gitter.im/engula/contributors?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

Engula is a storage engine that empowers engineers to build reliable and cost-effective databases with less effort and more confidence.

Engula's design goals are as follows:

- Elastic: unbundles components to leverage elastic resources on the cloud
- Adaptive: optimizes its own data structures according to dynamic workloads
- Extensible: provides flexible APIs and components to build higher-level applications
- Platform independent: supports flexible deployments on local hosts, on-premise servers, and cloud platforms

**Welcome to review [the design](docs/design.md) and discuss with us.**

Engula is in the demo stage now. We have planned two demos:

- Demo 1 explores the path towards an elastic storage engine
- Demo 2 explores the path towards an adaptive storage engine

**Demo 1 has released in Oct 2021. Please check [the report](https://engula.com/posts/demo-1/) for more details.**

**Demo 2 is now in progress and will release in Nov 2021. Please check [the roadmap](https://github.com/engula/engula/discussions/29) for more details.**

## Run Experiments

### Build

To build the benchmark tool:

```
cargo build --release
```

When the build finishes, you can find the binary in `target/release/engula`.

### Run

`engula` consists of two sub-commands: `start` and `bench`.

#### Run a Component

The `start` command allows you to run a component as a gRPC service.
`engula` provides four components: journal, storage, manifest, and compaction.

For example, to run the journal service at a specific address with a configuration file:

```
engula -c default.toml start journal --addr 0.0.0.0:10001
```

#### Run the Benchmark

The `bench` command allows you to run benchmarks against specific components.
There are two kinds of benchmarks: `put` and `get`.

The `put` benchmark runs concurrent tasks to put entries to the database.
The `get` benchmark runs concurrent tasks to get entries from the database.
The number of tasks, entries, and the size of values are all configurable in the configuration file.

In addition, there are two kinds of components: local and remote.
Local components run in the same process with the benchmark tool (embedded).
Remote components run in a standalone process and communicate with the benchmark tool through gRPC.

The benchmark tool runs local components by default.
For example, to run the `put` benchmark:

```
engula -c default.toml bench --put
```

You can specify the URL of individual remote components to benchmark against them.
For example, to benchmark against the journal we started above, specify its URL in the configuration file:

```
journal_url = "http://127.0.0.1:10001"
```

Please check `engula -h` and [default.toml](https://github.com/engula/engula/blob/demo-1/engula/etc/default.toml) for more details about usages and configurations.

### About the Report

To run experiments on [the report](https://engula.com/posts/demo-1/), you need to do the following steps:

- Provision EC2 instances according to the report.
- Build the benchmark tool and deploy it to specific instances.
- Run the corresponding components and benchmarks on the specific instances with the provided configuration files in [engula/etc](https://github.com/engula/engula/tree/demo-1/engula/etc).

Of course, you can also try to play with the benchmark tool in different ways, have fun :)

## Use the Storage Engine

You can run [the example](engula/examples/hello.rs) with:

```rust
use std::sync::Arc;

use engula::*;

#[tokio::main]
async fn main() -> Result<()> {
    let dirname = "/tmp/engula/hello";
    let _ = std::fs::remove_dir_all(dirname);

    println!(
        "🚧 Creating a hybrid storage under {} that reads from a sstable storage and \
        writes to a sstable storage and a parquet storage...",
        dirname
    );

    let fs = Arc::new(LocalFs::new(dirname)?);
    let sstable_options = SstableOptions::default();
    let sstable_storage = Arc::new(SstableStorage::new(fs.clone(), sstable_options));
    let parquet_options = ParquetOptions::default();
    let parquet_storage = Arc::new(ParquetStorage::new(fs.clone(), parquet_options));
    let storage = Arc::new(HybridStorage::new(
        sstable_storage.clone(),
        vec![sstable_storage, parquet_storage],
        None,
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

    println!("🚀 Putting to the database concurrently...");

    let num_tasks = 4u64;
    let num_entries = 256u64;
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

    println!("📜 Verifying the written entries...");

    for i in 0..num_entries {
        let v = i.to_be_bytes().to_vec();
        let got = db.get(&v).await.unwrap();
        assert_eq!(got, Some(v.clone()));
    }

    println!(
        "🏆 Successfully created a hybrid storage under: {}!",
        dirname
    );

    Ok(())
}
```
