use std::io::{Read, Write};

use crate::Result;

pub trait FileSystem {
    fn new_reader(fname: &str) -> Result<Box<dyn FileReader>>;

    fn new_writer(fname: &str) -> Result<Box<dyn FileWriter>>;
}

pub trait FileReader: Read {}

pub trait FileWriter: Write {}
