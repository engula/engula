# Single write journal

- Status: draft
- Pull Request:

## Abstraction

The luna engine needs a single-writing, multi-reading journal system.

## Design

### API

This has discussed in [#260](https://github.com/engula/engula/discussions/260). A trait named `SingleWriteJournal` is introduced to observe the role changes:

```rust
pub enum RoleState {
    Leader,
    Follower,
}

pub trait SingleWriteJournal : Journal {
    type Role;
    type Peer;
    type StateStream: Stream<Item = RoleState>;

    fn state(&self, name: &str) -> (Self::Role, Option<Self::Peer>);

    async fn observe_state(&self, name: &str) -> Self::StateStream;
}
```

The `SingleWriteJournal` doesn't effects the semantics of `Journal`, so `Journal::open_stream_writer` could be called whenever a stream isn't a leader. Of course, the implementation should guarantee that calls `StreamWriter::append` or others modifying operations will got a `Error::NotLeader`, if it isn't the stream leader.

### Architecture

![single write journal architecture](../images/single-write-journal-architecture.svg)

A `SingleWriteJournal` consists of a master, journal orchestrator, a set of journal server and a set of journal client which is parts of engine.

The journal server provides the durability of events of journal. All events are produced by journal client. At the same time, only one journal client could produce events which will be accepted by journal servers. That one is called leader, the others journal client are followers.

The master is responsible for electing new leader and detecting the leader's live. The master is also responsible for providing routers and balancing loads among journal servers. To scale the set of journal servers on-demand, the master provisions or de-provisions journal servers from the orchestrator.

#### Electing and Fault detecting

The master collects status and stats from both journal client and server periodic via heartbeat RPC requests. If master haven't received heartbeats from current leader after a while, it would choose a new client as new leader, a set servers as replication group, and assign a monotonic epoch to the new leader.

The order of events in an replication group is decided by leader. In order to ensure the consistency of orders of events, before committing any events, a leader should ensure there no any events which produced by former leader would be accepted by journal servers. Via a sealing RPC requests,  the new master requires all journal servers don't accepts any events with small epoch.

#### Replication Policy

A events will be replicated to all journal servers of an replication group eventually. Once a event is replicated to enough journal servers which is specified by the replication policy, the event could be commit and apply to engine.

#### Reconfiguration

In general, an replication policy allows that a journal server downtime unexpectedly, but not effects the writing operations. In order to keep the availability in this situition, master would enforce leader to seal previous events and allocate new epoch so that it could change the configuration such as replication group to remove the faulted nodes.

#### Follower read

A leader will broadcast the committed sequence of events to all journal server, and those events is visible for reading. But here exists a gap between a event become committed in leader and a event is readable in a journal server. So a follower want to read events with consistency, it should ask the latest committed sequence from leader and wait until it receive those events.

### Future works

#### Chain replication

A leader might be the bottleneck, since it is responsible for replicating events to all journal servers. We could employs the chain replication mechanism that allow journal servers replicate events to other servers. Specially, a leader could use chain replication to replicate events to all followers.

#### Archive

After a series of events are sealed, those events could be put into s3 to reduce usage of local disk. Specially, user could manually archive some events to a cheap stores.
