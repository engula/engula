# Engula

This document describes the top-level design of Engula.

Engula is a distributed key-value store.

## Data model

An Engula deployment is called a cluster. A cluster serves multiple databases, each of which consists of multiple collections. A collection contains a set of key-value pairs that are partitioned (hash or range) into shards. Keys are the unit of data partition, and shards are the unit of data movement.

## Architecture

![Architecture](architecture.drawio.svg)

## Bootstrap

A cluster is bootstrapped when the first node is started. The first node creates a root group (group 0).

## Root group

A root group is the first group created when a cluster is bootstrapped. A root group has group id 0.

A root group serves an internal database named system. The system database consists of some collections:

- nodes: contains node descriptors
- groups: contains group descriptors
- databases: contains database descriptors
- collections: contains collection descriptors

A root group manages various parts of a cluster as follows.

## Node management

### Add a node

To add a node, a client sends a node descriptor to the root group. The root group allocates a unique node id to the node and adds the node descriptor to the nodes collection.

### Remove a node

To remove a node, a client sends the target node id to the root group. The root group registers a task to move replicas from the target node to others. When the task finishes, the root group removes the target node descriptor from the nodes collection.

## Group management

### Create a group

To create a group, the root group allocates a unique group id and selects a few nodes to create one replica on each node. Then the root group adds the group descriptor to the groups collection. If the root group fails before the group descriptor is added, the residual replicas will be garbage collected.

## Shard management

Shards are created when collections are created. Shards of the same collection in the same group can be split or merged. Shards can also be moved among different groups.

Groups contain the single point of truth about shards?

### Add a shard

To add a shard, the root group sends a shard descriptor to a data group. The data group then replicates the shard descriptor to its replicas.

## Database management

### Create a database

To create a database, a client sends a database descriptor to the root group. The root group allocates a unique database id to the database and adds the database descriptor to the databases collection.

## Collection management

### Create a collection

To create a collection, a client sends a collection descriptor to the root group. The root group allocates a unique collection id to the collection and adds the collection descriptor to the collections collection.

A collection consists of at least one shard. When a collection is created, the root group also creates one or more shards for the collection and then assigns the shards to some groups before the collection is available.

## Load balance

The root group pulls all nodes to collection statistics about nodes and groups.

## Group

A replication group consists of a group of replicas. Each group serves one or more shards.

## Store

A node stores all data in a single local store.

A store consists of multiple tables. Each collection in a node is stored in an individual table.