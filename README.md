# [Engula](https://engula.io)

[![Zulip][zulip-badge]][zulip-url]
[![Twitter][twitter-badge]][twitter-url]

[zulip-badge]: https://img.shields.io/badge/Zulip-chat-brightgreen?style=flat-square&logo=zulip
[zulip-url]: https://engula.zulipchat.com
[twitter-badge]: https://img.shields.io/twitter/follow/engulaio?style=flat-square&logo=twitter&color=brightgreen
[twitter-url]: https://twitter.com/intent/follow?screen_name=engulaio

Engula is a persistent data structure store, used as a database and storage engine. Engula aims to be the standard collections for stateful applications.

The features of Engula include:

- Provide a set of persistent data structures.
- Support ACID transactions.
- Implement a built-in cache tier to speed up reads, resist hotspots and burst traffic.
- Implement a cloud-native architecture to deliver a cost-effective, highly-scalable, and highly-available service on the cloud.

## Status

We are working on v0.3. Please check the [roadmap][roadmap] for more details.

[roadmap]: https://github.com/engula/engula/issues/359

We released demo 1 in Oct 2021 and v0.2 in Dec 2021. You can check the [demo 1 report](https://engula.com/posts/demo-1/) and [v0.2 release post](https://engula.io/posts/release-0.2/) for more details.

## Examples

You can check some usages in [examples](src/client/examples).

To run the examples:

```
cargo run -p engula -- server start
cargo run -p engula-client --example universe
cargo run -p engula-client --example collection
```

## Information

For internal designs, please see the [docs](docs). For informal discussions about plans, ideas, and designs, please go to the [discussion forum](https://github.com/engula/engula/discussions).

## Contributing

Thanks for your help in improving the project! We have a [contributing guide](CONTRIBUTING.md) to help you get involved in the Engula project.
