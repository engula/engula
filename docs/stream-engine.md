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

All events are replicated to the last segment. When a segment meets certain conditions, such as the size exceeds the threshold, leadership transferred (introduced in [leader election](#leader-election)), it would be **sealed** (introduced in [recovery](#recovery)) and a new segment is created to serve new events.

#### Durable

TODO

#### Fault Tolerance

##### Leader Election

##### Recovery

### Data Path

#### Replication Policy

### Master

### Store

#### Accumulated Ack

## Future Work

