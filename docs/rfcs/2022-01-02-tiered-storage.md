# Tiered Storage

- Status: draft
- Pull Request: https://github.com/engula/engula/pull/242

## Abstract

This document proposes a tiered storage abstraction. A tiered storage is an object storage that stores data in multiple storage tiers to provide different cost-performance tradeoffs. It supports flexible options to allow users to decide how to store and access their data.

## Semantic

A tiered storage consists of a hot tier and a cold tier. The hot tier offers faster data access with higher storage costs. The cold tier offers slower data access with lower storage costs.

A tiered storage organizes objects in buckets and provides the following interfaces:

```rust
trait TieredStorage {
    /// Creates a bucket.
    fn create_bucket(&self, bucket);
    /// Deletes a bucket.
    fn delete_bucket(&self, bucket);
    /// Lists buckets in the storage.
    fn list_buckets(&self);
    /// Returns a description about the given bucket.
    fn describe_bucket(&self, bucket);
    /// Lists objects in the given bucket.
    fn list_objects(&self, bucket);
    /// Returns a description about the given object.
    fn describe_object(&self, bucket, object);
    /// Returns an object reader for random reads.
    fn new_random_reader(&self, bucket, object, options: &ReadOptions);
    /// Returns an object reader for sequential reads.
    fn new_sequential_reader(&self, bucket, object, options: &ReadOptions);
    /// Returns an object writer for random writes.
    fn new_random_writer(&self, bucket, object, options: &WriteOptions);
    /// Returns an object writer for sequential writes.
    fn new_sequential_writer(&self, bucket, object, options: &WriterOptions);
}

enum Tier {
    Hot,
    Cold,
}

struct ReadOptions {
    /// Whether to store data in the hot tier to speed up future reads.
    warmup_data: bool,
    /// The preferred tier to read.
    preferred_tier: Option<Tier>,
}

struct WriteOptions {
    /// The preferred tier to write.
    preferred_tier: Option<Tier>,
}
```

## Use cases

An LSM-tree storage engine performs both foreground and background IOs. Foreground IOs are latency-sensitive, so they should access the hot tier. Background IOs are not latency-sensitive, so they can access the cold tier to avoid interfering with foreground IOs.
