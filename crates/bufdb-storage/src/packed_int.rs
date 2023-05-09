use std::fmt::Display;
use std::io::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct PackedI32(pub i32);

impl PackedI32 {
    pub const MAX_LENGETH: usize = 5;

    pub fn write(&self, buf: &mut [u8]) -> Result<usize> {
        let (mut val, negative) = if self.0 < -119 {
            (- self.0 - 119, true)
        } else if self.0 > 119 {
            (self.0 - 119, false)
        } else {
            buf[0] = self.0 as _;
            return Ok(1);
        };

        let mut len: usize = 0;
        while val & 0xFF != 0 && len <= 4 {
            buf[len + 1] = val as u8;
            len = len + 1;
            val = val >> 8;
        }

        buf[0] = if negative { (- 119 - len as isize) as _ } else { (119 + len) as _ };

        Ok(len)
    }

    pub fn read(&mut self, buf: &[u8]) -> Result<usize> {
        todo!()
    }
}

impl Display for PackedI32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for PackedI32 {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl Into<i32> for PackedI32 {
    fn into(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct PackedI64(pub i64);

impl PackedI64 {
    pub const MAX_LENGETH: usize = 9;

    pub fn write(&self, buf: &mut [u8]) -> Result<usize> {
        todo!()
    }

    pub fn read(&mut self, buf: &[u8]) -> Result<usize> {
        todo!()
    }
}

impl From<i64> for PackedI64 {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl Into<i64> for PackedI64 {
    fn into(self) -> i64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_byte() {
        let v1 = -127i8;
        let v2 = v1 as u8;
        let v3 = v2 as i8;

        println!("{}, {}, {}", v1, v2, v3);

        let v4 = -127i32;
        let v5 = v4 as i8;
        let v6 = v4 as u8;
        let v7 = v5 as u8;
        let v8 = v7 as i8;
        println!("{}, {}, {}, {}, {}", v4, v5, v6, v7, v8);

        let v9 = 0x10Fi16;
        let v10 = v9 as i8;
        println!("{}, {}", v9, v10);
    }
}