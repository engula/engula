use microunit::proto::UnitDesc;
use microunit::{Unit, UnitHandle};

pub struct SupremeUnit {}

impl SupremeUnit {
    pub fn new() -> SupremeUnit {
        SupremeUnit {}
    }
}

impl Unit for SupremeUnit {
    fn kind(&self) -> String {
        "supreme".to_owned()
    }

    fn spawn(&self, uuid: String) -> Box<dyn UnitHandle> {
        Box::new(SupremeHandle::new(uuid))
    }
}

struct SupremeHandle {
    unit: UnitDesc,
}

impl SupremeHandle {
    fn new(uuid: String) -> SupremeHandle {
        let desc = UnitDesc {
            uuid,
            kind: "supreme".to_owned(),
        };
        SupremeHandle { unit: desc }
    }
}

impl UnitHandle for SupremeHandle {
    fn desc(&self) -> UnitDesc {
        self.unit.clone()
    }
}
