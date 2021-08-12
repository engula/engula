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
