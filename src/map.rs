use std::borrow::Borrow;
use std::marker::PhantomData;

use crate::object::Object;
use crate::Result;

pub struct MapObject<I, O> {
    _id: PhantomData<I>,
    _ob: PhantomData<O>,
}

impl<I, O: From<Object>> From<Object> for MapObject<I, O> {
    fn from(_: Object) -> MapObject<I, O> {
        MapObject {
            _id: PhantomData,
            _ob: PhantomData,
        }
    }
}

impl<I, O: From<Object>> MapObject<I, O> {
    pub fn at<Q: ?Sized>(&self, _: &Q) -> O
    where
        I: Borrow<Q>,
    {
        let ob = Object {};
        ob.into()
    }
}
