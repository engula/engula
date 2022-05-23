# Engula

This document describes the top-level design of Engula.

Engula is a distributed key-value store.

## Data model

An Engula deployment is called a cluster. A cluster serves multiple databases, each of which consists of multiple collections. A collection contains a set of key-value pairs that are partitioned (hash or range) into shards. Keys are the unit of data partition, and shards are the unit of data movement.

## Architecture

![Architecture](architecture.drawio.svg)

## Bootstrap

A cluster is bootstrapped when the first node is started. The first node creates an init group (group 0).

## Init group

An init group is the first group created when a cluster is bootstrapped. An init group has group id 0.

An init group serves an internal database named system. The system database consists of some collections:

- nodes: contains metadata of each node
- databases: contains metadata of each database