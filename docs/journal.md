# Journal

This document describes the top-level design of Journal.

Journal provides an abstraction to store event streams.
Journal can be used as a standalone component or integrated with Engula.

## Semantics

Journal divides data into streams.
A stream stores a sequence of events.
Each stream has a unique identifier called the stream name.
Each event within a stream is associated with a unique timestamp.
Users should assign an increasing timestamp to events when appending to a stream.
However, timestamps within a stream are not required to be continuous, which allows users to dispatch events to multiple streams.

Journal provides the following interfaces to manipulate streams:

- List streams
- Create a stream with a unique name
- Delete a stream

Journal provides the following interfaces to manipulate events in a stream:

- Read events since a timestamp
- Append events with a timestamp
- Release events up to a timestamp

It is also possible to support stream subscriptions. We leave the exploration of this feature to future work.

Released events can be garbage collected or archived.
Whether released events are readable depends on the implementation.
For example, if events are archived, it should allow users to recover from archives.
Nevertheless, implementations should guarantee to return continuous events. That is, the returned events must be a sub-sequence of a stream.

## Guidelines

![Architecture](images/journal-architecture.drawio.svg)

Journal can be implemented in the following forms:

- Local Journal: a module that stores data in memory or file system.
- Remote Journal: a client that stores data in multiple remote services.
- External Journal: a client that stores data in various third-party services.

Journal doesn't assume how data should be persisted.
It is up to the implementer to decide what guarantees it provides.
Users can choose an appropriate implementation for their applications.

## Discussions

Casual discussions about the design and implementation should proceed in [this discussion][journal-discussion].
Formal discussions about the design of a specific implementation should proceed with an RFC.

[journal-discussion]: https://github.com/engula/engula/discussions/70
