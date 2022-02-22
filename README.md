# [Engula](https://engula.io)

[![Zulip][zulip-badge]][zulip-url]
[![Twitter][twitter-badge]][twitter-url]

[zulip-badge]: https://img.shields.io/badge/Zulip-chat-brightgreen?style=flat-square&logo=zulip
[zulip-url]: https://engula.zulipchat.com
[twitter-badge]: https://img.shields.io/twitter/follow/engulaio?style=flat-square&logo=twitter&color=brightgreen
[twitter-url]: https://twitter.com/intent/follow?screen_name=engulaio

Engula is a persistent data structure store, used as a database and storage engine. Engula aims to be the definitive data collection for stateful applications.

Features:

- Provide data structures such as numbers, strings, maps, and lists.
- Support ACID transactions with different isolation and consistency levels.
- Provide built-in cache to speed up reads, resist hotspots and traffic bursts.
- Implement a cloud-native, multi-tenant architecture to deliver a cost-effective service.

## Status

We are working on v0.3. Please check the [roadmap][roadmap] for more details.

[roadmap]: https://github.com/engula/engula/issues/359

We released demo 1 in Oct 2021 and v0.2 in Dec 2021. You can check the [demo 1 report](https://engula.com/posts/demo-1/) and [v0.2 release post](https://engula.io/posts/release-0.2/) for more details.

## Examples

You can check some usages in [examples](src/client/examples).

To run the examples:

```
cargo run -p engula -- server start
cargo run -p engula-client --example database
cargo run -p engula-client --example {example file name}
```

## Contributing

Thanks for your help in improving the project! We have a [contributing guide](CONTRIBUTING.md) to help you get involved in the Engula project.

## More information

For internal designs, please see the [docs](docs). For informal discussions, please go to the [forum](https://github.com/engula/engula/discussions).
