# ![Engula](https://engula.com/images/logo-wide.png)

[![Gitter](https://badges.gitter.im/engula/contributors.svg)](https://gitter.im/engula/contributors?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

Engula is a cloud-native storage engine for next-generation data infrastructures.

Engula is in the demo stage now, welcome to **review [the design](docs/design.md)** and join [the room](https://gitter.im/engula/contributors) to discuss with us.
You can also **contact careers@engula.com to become a full-time developer!**

## Demo

```rust
use engula::{ListObject, MapObject, Uint64Object, Universe};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uv = Universe::open("engula://mem")?;
    let db = uv.create_database("demo")?;
    let co = db.create_collection("demo")?;
    let num: Uint64Object = co.object("a");
    num.add(1)?;
    num.sub(2)?;
    let list: ListObject<Uint64Object> = co.object("b");
    let ob = list.at(1);
    ob.add(1)?;
    let map: MapObject<String, Uint64Object> = co.object("c");
    let ob = map.at("abc");
    ob.add(1)?;
    Ok(())
}
```
