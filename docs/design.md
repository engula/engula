# Design

## What is Engula?

Engula is a distributed key-value store, used as a cache, database, and storage engine.

It has the following advantages:

- Elasticity

    engula supports automatic data migration and balancing, which allows you easily to scale up or down based on you application needs.

- Fault tolerance

    engula replicates data to multiple machines, the raft consensus is employed to ensure consistency between replicas and provide automatically fault recovery.

- Easy to use

    engula is architected and designed with ease of use in mind. In deployment, there is only one binary, the topology is simple, and other nodes such as proxy, master, etc. are not needed. In addition, engula automatically completes resource configuration as much as possible without requiring users to configure various parameters.

## Architecture

In order to reduce the complexity of deployment and operation and maintenance, all the functions of engula are concentrated on a binary.

![topology][topology]

[topology]: ./img/topology.drawio.svg

As shown in the figure above, all nodes in a cluster are engula servers, which internally coordinate and elect nodes that serve metadata.

In engula, a replication set is called a **Group**. A leader is elected within a Group to serve read and write requests targeting this Group. Groups can be migrated between servers to achieve load balancing.

![architecture][architecture]

[architecture]: ./img/architecture.drawio.svg

Group 0 is called the **Root Group**, which is responsible for balancing, schema maintenance, and providing routing information.

Apart from the root, the storage part of the engula server is called **Node**. Node is responsible for data access, log replication and shard migration. (A shard is the smallest granularity of data and will be introduced later.)

In addition, node is also responsible for providing group-level scheduling, including automatic maintaining num of replicas, automatic replace bad replicas, and replica migration between servers.

## Data organization

![data organization][data-organization]

[data-organization]: ./img/data-organization.drawio.svg

Engula provides two levels of data organization: database and collection.

Database is used to provide isolation between tenants; collection is used to provide data isolation within tenants.

A collection is partitioned into **Shards** to support elastic scale up or down. Currently engula supports both range and hash partitions. Shard splitting and merging will be introduced in subsequent releases.

Shards are managed by groups, and the replicas of group is also replicas of shards.

![group and shard][group-and-shard]

[group-and-shard]: ./img/group-and-shard.drawio.svg

As shown in the figure above, a group may be responsible for managing multiple shards. Shards can also be migrated online between groups.
