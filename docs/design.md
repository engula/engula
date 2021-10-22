# About

Status: Draft

This document describes the top-level design of Engula.

This document is evolving from [the previous design document][demo-1-url].
You can check that document for more details before this document is thorough.

[demo-1-url]: https://github.com/engula/engula/blob/demo-1/docs/design.md

# Architecture

![Architecture](images/architecture.drawio.svg)

Engula employs an unbundled architecture.
Engula unbundles the storage engine into five components: Compute, Journal, Storage, Manifest, and Background.
