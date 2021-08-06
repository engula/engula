# Engula

This document describes the top-level design of Engula.

## Principles

The design principles of Engula are as follows:

- Avoid designs that are too complicated for long-term support.
- Avoid external dependencies that are not built-in the platforms.

## Data Model

Engula exposes a semi-structured data model with rich data types to support diverse applications.

An Engula deployment is called a universe.
A universe contains multiple databases, which in turn contain multiple collections.
A collection is a set of key-value records versioned with timestamps.

Record keys are sequences of bytes, while record values can be of various types.
Engula supports primitive types (numbers, strings), compound types (unions, structs), and collection types (maps, lists).
Since some real-world applications need to store lots of elements in a single record, Engula will optimize records of collection types that contain millions of elements.

Engula supports atomic updates of a single record and ACID transactions across records within a database.
However, records of different databases are independent of each other, which means that interoperations between different databases are not possible.

## Architecture

Engula is a general-purpose storage engine that runs on various platforms.
While the detailed design of Engula depends on a specific platform, this document describes the general cloud-native architecture.
In this document, we assume that the running platform supplies elastic resources and provides APIs to provision and de-provision nodes.

For more details about the design and implementation of Engula on a specific platform, see the following documents:

- Embedded Storage Engine
- Amazon Web Services

### Unit

Engula employs a microunits architecture to take advantage of elastic resources.
Engula decomposes its functionalities into different kinds of units:

- Supreme Unit
- Central Unit
- Compute Unit
- Journal Unit
- Storage Unit
- Background Unit

These units have different resource characteristics.
For example, some are CPU-bound while some are IO-bound.
The decomposition allows Engula to allocate appropriate resources to different units according to their characteristics.

A unit runs on a node and possesses a certain amount of resources (e.g. CPU, IO) on the node.
A group of units can form a replication group to provide reliable service.
A replication group runs the Paxos consensus algorithm and elects a leader to process commands.
The details about Paxos can be found in related papers and will not be further discussed in this document.

**TODO: unit list, recover, tags**

The overall architecture and the behaviors of individual units are described below.

### Universe

An Engula universe consists of a set of nodes provisioned from the running platform.
These nodes serve as a unified resource pool for the universe.
Each node manages a set of units and exposes APIs to provision and de-provision units on demand.
The universe architecture is as follow:

![Universe Architecture](images/2021-08-01-universe-architecture.drawio.svg)

When a universe is bootstrapped, a group of supreme units is created.
These supreme units form a replication group that is capable of fail-over and self-repair.
If the leader supreme unit fails, the followers will start elections to elect a new leader.
If one follower supreme unit fails, the leader will provision a new supreme unit to replace the broken one.

A supreme unit stores a replica of the universe metadata on the local file system.
The leader supreme unit is responsible for the universe and manages the metadata and databases for it.

### Database

An Engula database consists of four kinds of units: central units, compute units, journal units, and storage units.
Units of different databases do not interact with each other at all.
The relationship between units of a database is as follow:

![Database Architecture](images/2021-08-01-database-architecture.drawio.svg)

When a database is created, the leader supreme unit registers the database and then provisions a group of central units to manage it.
These central units form a replication group that is capable of fail-over and self-repair like the supreme units.

A central unit stores a replica of the database metadata on the local file system.
The leader central unit is responsible for the database and manages the metadata and a dedicated set of units for it.
Although the central unit manages different kinds of units in different ways, some common principles are as follows:

**TODO**

- balance
- health-check and repair

The persistent state of a database consists of two parts: the journal and the storage.
The journal is a log system that stores logs and the storage is a file system that stores immutable files.

#### Journal

The journal architecture is as follow:

![Journal Architecture](images/2021-08-01-journal-architecture.drawio.svg)

Logs of a database can be divided into one or more shards.
Each shard manages one or more hash or range partitions of the database.

A group of journal units is responsible for the storage of one shard.
These journal units act as acceptors and learners in the consensus algorithm.
A journal unit stores logs on the local file system and exposes APIs to manipulate the logs.
The journal unit employs asynchronous IO and group commit to persist logs with minimal CPU consumption.

A group of compute units is responsible for the compute of one or more shards.
The leader compute unit acts as the distinguished proposer in the consensus algorithm.

**TODO: split, merge, transfer**

#### Storage

The storage architecture is as follow:

![Storage Architecture](images/2021-08-01-storage-architecture.drawio.svg)

Files of a database include a manifest and a set of data files organized into collections.
The manifest records the file layout of each collection in the database.
Collections can choose varied file structures to optimize for different workloads.

A manifest consists of a manifest file and a manifest journal.
The manifest file records the base version of the manifest.
The manifest journal records a sequence of version edits on the base version.
When the size of the manifest journal reaches a threshold, the manifest journal is merged with the manifest file to form a new manifest file.

The manifest file and all data files are immutable and replicated in the storage units, while the manifest journal is replicated in the central units for incremental updates.
The leader central unit is responsible to maintain the manifest and distribute files among storage units.

A storage unit stores immutable files on the local file system and exposes APIs to add, drop, and read these files.
The storage unit relies on the cache of the file system and doesn't introduce an additional cache.
The storage unit is designed to be as reliable and cost-effective as possible.
We leave performance optimization to the upper level, which has more application context to make better strategies.

The storage unit also records access statistics for each file and reports them to the central unit.
The central unit can calculate the hotness of files from these statistics to balance file distribution.
For example, the central unit can add more replicas for hot files in the fast storage tier to share traffics, while keeping cool files in the slow storage tier to save cost.

In addition, the central unit can schedule background jobs to reorganize files in storage units.
For example, the central unit can schedule compactions to merge files with overlapped ranges to improve read performance.
The central unit can also schedule compressions or garbage collections to reduce storage usage.
When a background job is scheduled, the central unit provisions a background unit to run it.
Since the background job is fault-tolerant and will not affect foreground services even if it fails, the background unit can be run with unreliable but cheap resources to save cost.

#### Execution

The leader compute unit is responsible to process client commands for the shards it manages.
The command execution flow is as follow:

![Compute Architecture](images/2021-08-01-compute-architecture.drawio.svg)

To handle writes, the compute unit replicates the updates to journal units of the corresponding shard.
Then the compute unit applies the updates to the memtable.
When the size of the memtable reaches a threshold, the memtable is flushed to the storage and a memtable is created.

To handle reads, the compute unit merges updates in the write buffer with data from the local cache or the remote storage.
The compute unit queries data from the local cache first.
If the required data is not in the local cache, the compute unit reads from the remote storage instead and then fills the local cache.

To further improve performance, the following optimizations can be introduced:

- Aggregate concurrent commands of a single record and execute them at once to deal with single-point hotspots.

**TODO: transaction**
