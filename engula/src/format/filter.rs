pub trait FilterReader {
    fn may_exist(&self, key: &[u8]) -> bool;
}

pub trait FilterBuilder {
    fn add(&mut self, key: &[u8]);

    fn finish(&mut self) -> Vec<u8>;
}
