# ![Engula](https://engula.com/images/logo-wide.png)

[![Gitter](https://badges.gitter.im/engula/contributors.svg)](https://gitter.im/engula/contributors?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

Engula is a cloud-native storage engine for next-generation data infrastructures.

Engula is in the demo stage now, welcome to **review [the design](docs/design.md)** and **join [the room](https://gitter.im/engula/contributors)** to discuss with us.
You can also **contact careers@engula.com to become a full-time developer!**

## Usage

1. Install `engula`:

```
cargo install engula
```

2. Run an Engula node:

```
engula node init
```

```
node 8b23f970-542e-404d-b1a3-27130c87a8ea listen on 127.0.0.1:21812
```

3. In another terminal, lists all Engula nodes:

```
engula node list
```

```
[
    NodeDesc {
        uuid: "8b23f970-542e-404d-b1a3-27130c87a8ea",
    },
]
```