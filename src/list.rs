use std::marker::PhantomData;

use crate::object::Object;

pub struct ListObject<O> {
    _ob: PhantomData<O>,
}

impl<O: From<Object>> From<Object> for ListObject<O> {
    fn from(_: Object) -> ListObject<O> {
        ListObject { _ob: PhantomData }
    }
}

impl<O: From<Object>> ListObject<O> {
    pub fn at(&self, _: usize) -> O {
        let ob = Object {};
        ob.into()
    }
}
