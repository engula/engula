# Source Code Development Guide

Engula consists of three subsystems: `kernel`, `object-engine`, and `stream-engine`. Each subsystem consists of multiple crates that are all published to crates.io to make them work.

Among these crates, the `engula-apis` and `engula-client` are regarded as public and the others are private. Public crates should be backward compatible and should not depend on private crates. Private crates can depend on public and private crates at the same or a lower hierarchical level.

## kernel

Dependencies:

```
cmd
 |
transactor
 |
cooperator
 |
supervisor
 |
common
 |
object-engine/stream-engine
```

## object-engine

Dependencies:

```
client
  ||
master
  ||
lsmstore
  ||
filestore
  ||
common
```

## stream-engine

Dependencies:

```
client
  ||
master
  ||
store
  ||
common
```
