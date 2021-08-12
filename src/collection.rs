use crate::object::Object;

pub struct Collection {}

impl Collection {
    pub fn object<O: From<Object>>(&self, _: &str) -> O {
        let ob = Object {};
        ob.into()
    }
}
