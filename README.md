# ![Engula](https://engula.com/images/logo-wide.png)

[![Gitter](https://badges.gitter.im/engula/contributors.svg)](https://gitter.im/engula/contributors?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

Engula is a cloud-native storage engine for next-generation data infrastructures.

Engula is in the demo stage now, welcome to **review [the design](docs/design.md)** and join [the room](https://gitter.im/engula/contributors) to discuss with us.
You can also **contact careers@engula.com to become a full-time developer!**

## Demo

```rust
use engula::{Uint64Object, Universe};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uv = Universe::open("engula://mem")?;
    let db = uv.create_database("demo")?;
    let counters = db.create_collection("counters")?;
    let a: Uint64Object = counters.object("a");
    a.add(1)?;
    a.sub(2)?;
    Ok(())
}
```
