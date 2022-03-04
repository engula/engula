# Stream Engine

This document describes the detailed design of Stream Engine.

## Architecture

Stream Engine is a low latency, fault-tolerant, scalable and durable stream system. It servers as a single producer multiple consumer.

![Architecture](images/stream-engine-architecture.drawio.svg)

A Stream Engine deployment consists of a Master and a collection of Segment Stores.

The segment stores are responsible for providing data durability and subscription. The master is responsible for metadata access and persistence, balance load among stores, and election arbitration.

Some other mechanisms such as leader elections, events replication, and segment recovery are implemented in the client of Stream Engine, to achieve lower latency.

### Overview

Leaving the concept of multiple tenants aside, Stream Engine is responsible for maintaining a append only stream, which consists of a collection of **events** proposed by users.

#### Scalable

A stream's number of events is unrestricted, its size might exceeds the hardware limitation. In order to achieve scalability, a stream is divided into multiple **segments** and distributed across various segment stores.

All events are replicated to the last segment. When a segment meets certain conditions, such as the size exceeds the threshold, leadership transferred (introduced in [leader election and recovery](#leader-election)), it would be **sealed** (introduced in [recovery](#recovery)) and a new segment is created to serve new events.

#### Durable

![Stream and segments](images/stream-engine-segments.drawio.svg)

Hardware failures can happen at any time, and to ensure data durability, each one of events proposed by users is replicated to multiple segment stores. That is, each segment has multiple **replicas**, and the events are complete as long as enough replicas exists. The consistency between replicas is resolved by the [replication policy](#replication-policy). When a event is replicated to enough segment stores (config in replication policy), it is **acked** and could be read.

#### Fault Tolerance

Like the segment stores, the hardware where a stream engine client is running might fail at any time, and a standby client is needed to tolerant failures and achieve availability.

At the same time, a stream only have one client could append events (in other words, only one of events proposed in parallel could be acked). The different client are coordinated by **leader election**, and only the leader proposed events will be acked by stream engine.

##### Leader Election and Safety

Each client randomly initiates an election which is then arbitrated by the master. Periodically, a client sends a **heartbeat** request to master. The master will choose a client at random to grant leader lease to, and it will renew leader lease each time it receives a heartbeat from the leader. Within a lease, the master guarantees that another client will not be elected as the leader. If the master does not receive a heartbeat from the leader after the lease expires, it will re-elect a new client as the leader.

Sometimes due to network issues, the heartbeat of a leader does not always arrive at the master on time, so a new leader might be elected to serve, and the old leader unaware that it has lost its leader lease. Then two leaders will be replicating their events at the same time. In extreme cases, a request from old leader may arrive at the segment stores after a request from new leader. To solve this problem, the segment store introduces a mechanism similar to the fence token.

In addition to leases, each time a leader is elected, the master also assigns it a unique monotonically increasing number, termed **epoch**. Before starting to replicate events, a leader must send a **seal** request to segment stores, which includes the leader's epoch. Once the seal request has been received, the segment store will guarantee that it will not receive any requests with a small epoch.

This process is called **replica sealing**. Only the number of received requests satisfied the replication policy requirement, could the leader begin replicating events, as it ensured that no any events from previous epoch would be acked by stream engine.

##### Recovery

When a working leader goes down unexpectedly, the segment stores may contain some inconsistent replicas of events. In a majority replication policy, for example, the leader receives the responses from majority and returns success to the user by calculating the number of replicas (to achieve the lowest possible latency), and asynchronously broadcasting acked index to all segment stores. If that leader dies before broadcasting, the new leader will have to recalculate which events have been acked (as users may already be aware) in order to maintain external consistency.

In the "replica sealing" response, each segment store will carry they known acked index. Starting with the largest acked index, the new leader must read events from all segment stores and replicate them to other segment stores. Ultimately, all events are acked and no inconsistent replicas of events in the segment stores. Once the above steps have been completed, the segment can be marked as sealed by the leader. The process described in above is call **segment sealing** or **recovery**.

The recovery process could consume a lot of time, the new leader might not be able to ack events during the intervals. In addition to recovery, the leader allows events to be replicated in order to reduce latency. However, new events can only be acked if all previous segment have been sealed to maintain consistency. This is called **parallel replicating and recovering**. Only two unsealed segment are allowed here for simplicity. The leader will not be able to replicate events if there already exists two unsealed segments.


### Data Path

#### Replication Policy

### Store

#### Accumulated Ack

#### Entry and Epoch

Describe events and hole.

### Master

## Future Work

