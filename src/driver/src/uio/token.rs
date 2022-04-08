pub struct Token(pub u64);

impl Token {
    pub fn new(id: u64, op: u8) -> Token {
        Token(id << 8 | op as u64)
    }

    pub fn id(&self) -> u64 {
        self.0 >> 8
    }

    pub fn op(&self) -> u8 {
        self.0 as u8
    }
}
