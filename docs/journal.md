# Journal

This document describes the top-level design of Journal.

Journal provides an abstraction to store logs.
Journal can be used as a standalone component or integrated with Engula.

## Semantics

Journal divides logs into streams.
A stream stores a sequence of records.
Each stream has a unique identifier called the stream name.
Each record within a stream is associated with a unique sequence number.
Users should assign an increasing sequence number to records when appending to a stream.
However, sequence numbers within a stream are not required to be continuous, which allows users to dispatch records to multiple streams.

Journal provides the following interfaces to manipulate streams:

- List streams
- Create a stream with a unique name
- Delete a stream

Journal provides the following interfaces to manipulate records of a stream:

- Append records with a sequence number
- Release records up to a sequence number
- Read records started with a sequence number

Released records can be garbage collected or archived.
Whether released records are readable depends on the implementation.
Nevertheless, implementations should guarantee to return continuous records. That is, the returned records must be a sub-sequence of the stream.

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

Casual discussions about the design and implementation should be proceeded in the [forum][journal-discussion].
Formal discussions about the design of a specific implementation should be proceeded with an RFC document.

[journal-discussion]: https://github.com/engula/engula/discussions/70
