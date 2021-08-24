# ![Engula](https://engula.com/images/logo-wide.png)

[![Gitter](https://badges.gitter.im/engula/contributors.svg)](https://gitter.im/engula/contributors?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

Engula is a cloud-native storage engine that empowers engineers to build next-generation data infrastructures with less effort and more confidence.

Engula is in the demo stage now.
Check **[the roadmap](https://github.com/engula/engula/issues/1)** for more details.

Welcome to **review [the design](docs/design.md)** and **join [the room](https://gitter.im/engula/contributors)** to discuss with us.
We also offer full-time jobs.
Please get in touch with **careers@engula.com** for more information.

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

3. In another terminal, list all Engula nodes:

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

4. Create a `supreme` unit:

```
engula unit create --kind supreme
```

```
created unit Some(
    UnitDesc {
        uuid: "c61dc1c2-dfaf-4221-b9b8-7b1ac4fe6e4f",
        kind: "supreme",
    },
)
```

5. List all units:

```
engula unit list
```

```
[
    UnitDesc {
        uuid: "c61dc1c2-dfaf-4221-b9b8-7b1ac4fe6e4f",
        kind: "supreme",
    },
]
```