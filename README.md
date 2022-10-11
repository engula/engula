# [Engula](https://engula.io)

[![Zulip][zulip-badge]][zulip-url]
[![Twitter][twitter-badge]][twitter-url]

[zulip-badge]: https://img.shields.io/badge/Zulip-chat-brightgreen?style=flat-square&logo=zulip
[zulip-url]: https://engula.zulipchat.com
[twitter-badge]: https://img.shields.io/twitter/follow/engulaio?style=flat-square&logo=twitter&color=brightgreen
[twitter-url]: https://twitter.com/intent/follow?screen_name=engulaio

Engula is a distributed key-value store, used as a cache, database, and storage engine.

## Architecture

![topology][topology]

See [design doc][design-doc] for more details.

[topology]: ./docs/img/topology.drawio.svg
[design-doc]: ./docs/design.md

## Quick start

1. Build

```sh
make build
```

2. Deploy a cluster

```sh
bash scripts/bootstrap.sh setup
```

3. Verify

```sh
cargo run -- shell
```

Run and enjoy it.

## Contributing

Thanks for your help in improving the project! We have a [contributing guide](CONTRIBUTING.md) to help you get involved in the Engula project.

## More information

For informal discussions, please go to the [forum](https://github.com/engula/engula/discussions).
