# Leader Based Journal

- Status: accepted
- Discussion: https://github.com/engula/engula/discussions/260
- Pull Request: https://github.com/engula/engula/pull/280

## Summary

In this RPC, we present a trait `LeaderBasedJournal`, which divides the users of `Journal` into two roles: a leader who could write, and followers, who only have read permission. In the same time, this trait provides a means of observing role transition.

## Motivation

The luna engine requires a leader to execute mutations, such as journal writing, flushing memory tables into persisted storage, as well as a group of followers who subscribe journal streams and reply mutations, to remain consistent with the engine leader.

To fulfil the luna engine's requirements, the journal need a mechanism to collaborate with luna engine's electing. But the electing method is general and could be utilized by various engine implementations. used by other engine implementations, we decided to abstract a type of journal supports electing (leader-based journal).

In the abstraction of leader-based journals, we need to ensure that there is only one leader at any given moment, and we also need to offer a way for followers to subscribe to journal streams. Finally, we must create an interface for the engine to use in order for it to detect role changes and make appropriate judgments.

Furthermore, the new abstraction must be compatible with the existing journal abstraction in order for users to simply replace it.

## Design

Here is the API design of leader-based journal.

```rust
pub enum Role {
    Leader,
    Follower,
}

pub trait EpochState {
    fn epoch(&self) -> u64;

    // The role of associated stream.
    fn role(&self) -> Role;

    // The leader of the associated stream.
    fn leader(&self) -> Option<String>;
}

pub trait LeaderBasedJournal : Journal {
    type EpochState: EpochState;
    type StateStream: Stream<Item = Self::EpochState>;

    fn state(&self, name: &str) -> Result<Self::EpochState>;

    async fn observe_state(&self, name: &str) -> Result<Self::StateStream>;
}
```

The `LeaderJournal` doesn't affects the semantics of `Journal`, so `Journal::open_stream_writer` could be called whenever a stream isn't a leader. Of course, the implementation should guarantee that calls `StreamWriter::append` or other modifying operations will got a `Error::NotLeader`, if it isn't the stream leader.

The `LeaderBasedJournal` will forwards the electing progress automatically, which the engine won't have to recognize it. However, the engine must initiate that automatic progress manually, because a journal might contains multiple streams, which could exceeds the hardware limitation if we monitors all stream's electing progress. As a result, just streams that the engine is interested in will be watched.

When the engine calls `LeaderBasedJournal::observe_state`, the `LeaderBasedJournal` starts monitoring and subscribing to the electing state transition. It will yield a `Stream` that will be fired whenever one of the electing states changes. We utilize the epoch to track state changes. Time is divided into epochs of arbitrary length, the `LeaderBasedJournal` must ensure that each epoch has only one leader.

When a leader engine crashes, another machine's `LeaderBasedJournal` instance is elected as the new leader and begins to recover, eventually providing service.

We can't ensure that the state returned by the `observe_state` or `state` methods is always fresh in a distributed system, but any write operations will identify this circumstance. As a result, every decision made before submitting should trigger any write operations to check for freshness.
