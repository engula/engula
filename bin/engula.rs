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
