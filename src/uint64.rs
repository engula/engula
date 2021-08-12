use crate::object::Object;
use crate::Result;

pub struct Uint64Object {}

impl From<Object> for Uint64Object {
    fn from(_: Object) -> Uint64Object {
        Uint64Object {}
    }
}

impl Uint64Object {
    pub fn add(&self, v: u64) -> Result<u64> {
        Ok(v)
    }

    pub fn sub(&self, v: u64) -> Result<u64> {
        Ok(v)
    }
}
