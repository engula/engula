# Journal

This document describes the top-level design of Journal.

Journal provides an abstraction to store logs.
Journal can be used as a standalone component or integrated with Engula.

## Semantics

Journal divides logs into streams.
A stream is a sequence of records.
Each stream has a unique identifier called stream id.
Each record within a stream is associated with a unique sequence number.
Users should assign an increasing sequence number to records when appending to a stream.
However, sequence numbers within a stream are not required to be continuous, which allows users to dispatch records to multiple streams.

Journal provides the following interfaces to manipulate a stream:

- Read records since a sequence number
- Append records with a sequence number
- Release records up to a sequence number

## Architecture

![Architecture](images/journal-architecture.drawio.svg)

Journal can be implemented in the following forms:

- Local Journal: a module that stores data in memory or file system.
- Remote Journal: a client that stores data in one or more remote services.
- External Journal: a client that stores data in various third-party services.

Journal doesn't assume how data should be persisted or replicated.
It is up to the implementer to decide what guarantees it provides.
Users can choose the best-fit implementation for their applications.

## Discussions and Implementations

Casual discussions about the design and implementations should be proceeded in the [forum][journal-discussion].
Formal discussions about the design of a specific implementation should be submitted as an RFC.

[journal-discussion]: https://github.com/engula/engula/discussions/70