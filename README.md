# [Engula](https://engula.io)

[![Zulip][zulip-badge]][zulip-url]
[![Twitter][twitter-badge]][twitter-url]

[zulip-badge]: https://img.shields.io/badge/Zulip-chat-brightgreen?style=flat-square&logo=zulip
[zulip-url]: https://engula.zulipchat.com
[twitter-badge]: https://img.shields.io/twitter/follow/engulaio?style=flat-square&logo=twitter&color=brightgreen
[twitter-url]: https://twitter.com/intent/follow?screen_name=engulaio

Engula is a cloud-native data structure store, used as a database, cache, and storage engine.

Features:

- Provide data structures such as numbers, strings, maps, and lists.
- Support ACID transactions with different isolation and consistency levels.
- Provide built-in cache to speed up reads, resist hotspots and traffic bursts.
- Implement a cloud-native, multi-tenant architecture to deliver a cost-effective service.

## Usage

The current released version is 0.3. You can start with this [tutorial][tutorial].

[tutorial]: https://www.engula.io/blog/tutorial-0.3

You can also check more usages in [examples](src/client/examples). To run the examples:

```
cargo run -p engula -- server start
cargo run -p engula-client --example tutorial
cargo run -p engula-client --example {example file name}
```

## Status

We are working on v0.4. Please check the [roadmap][roadmap] for more details. For previous releases, please check the release posts on the [website][website].

[roadmap]: https://github.com/engula/engula/issues/490
[website]: https://engula.io/blog

## Contributing

Thanks for your help in improving the project! We have a [contributing guide](CONTRIBUTING.md) to help you get involved in the Engula project.

## More information

For internal designs, please see the [docs](docs). For informal discussions, please go to the [forum](https://github.com/engula/engula/discussions).
