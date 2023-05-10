use std::fmt::Display;
use std::io::Result;

/// Packed `i32` storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct PackedI32(pub i32);

macro_rules! pack_write {
    ($self: ident, $buf: expr) => {
        {
            let (mut val, negative) = if $self.0 < -119 {
                (($self.0 + 119).abs(), true)
            } else if $self.0 > 119 {
                ($self.0 - 119, false)
            } else {
                $buf[0] = $self.0 as _;
                return Ok(1);
            };
    
            let mut len: usize = 0;
            while val != 0 {
                len = len + 1;
                $buf[len] = val as u8;
                val = val >> 8;
            }
    
            $buf[0] = if negative { (- 119 - len as isize) as _ } else { (119 + len) as _ };
    
            Ok(len + 1)    
        }
    };
}

macro_rules! pack_read {
    ($self: ident, $buf: expr, $t: ty) => {
        {
            let v = $buf[0] as i8;
            let (negative, len) = if v < -119 {
                (true, (v + 119).abs() as usize)
            } else if v > 119 {
                (false, (v - 119) as usize)
            } else {
                $self.0 = v as $t;
                return Ok(1);
            };
    
            let mut val = <$t>::default();
            for i in (1..=len).rev() {
                val = (val << 8) + $buf[i] as $t;
            }
    
            $self.0 = if negative { - val - 119 } else { val + 119 };
    
            Ok(len + 1)
        }
    };
}

impl PackedI32 {
    pub const MAX_LENGETH: usize = 5;

    pub fn write(&self, buf: &mut [u8]) -> Result<usize> {
        pack_write!(self, buf)
    }

    pub fn read(&mut self, buf: &[u8]) -> Result<usize> {
        pack_read!(self, buf, i32)
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

/// Packed `i64` storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct PackedI64(pub i64);

impl PackedI64 {
    pub const MAX_LENGETH: usize = 9;

    pub fn write(&self, buf: &mut [u8]) -> Result<usize> {
        pack_write!(self, buf)
    }

    pub fn read(&mut self, buf: &[u8]) -> Result<usize> {
        pack_read!(self, buf, i64)
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
    use crate::packed_int::PackedI64;

    use super::PackedI32;

    macro_rules! check {
        ($v: ident, $len: ident, $t: ty) => {
            {
                let mut buf = [0u8; <$t>::MAX_LENGETH];

                let val = <$t>::from($v);
                let size = val.write(&mut buf).unwrap();
                if let Some(len) = $len {
                    assert_eq!(len, size, "write len error");
                }
                let mut ret = <$t>::default();
                let ret_len = ret.read(&buf).unwrap();
                assert_eq!(size, ret_len, "read len error");
                assert_eq!(val, ret, "read value error");
            }            
        };
    }

    fn check_i32(v: i32, len: Option<usize>) {
        check!(v, len, PackedI32)
    }

    fn check_i64(v: i64, len: Option<usize>) {
        check!(v, len, PackedI64)
    }

    #[test]
    fn test_packed_i32() {
        check_i32(0, Some(1));
        check_i32(1, Some(1));
        check_i32(-1, Some(1));
        check_i32(119, Some(1));
        check_i32(-119, Some(1));
        check_i32(120, Some(2));
        check_i32(-120, Some(2));
        check_i32(i32::MAX, Some(5));
        check_i32(i32::MIN, Some(5));
        check_i32(18, None);
        check_i32(189, None);
        check_i32(1834325324, None);
        check_i32(770, None);
        check_i32(-18, None);
        check_i32(-189, None);
        check_i32(-1834325324, None);
        check_i32(-770, None);
        check_i32(0xf010, None);
    }

    #[test]
    fn test_packed_i64() {
        check_i64(0, Some(1));
        check_i64(1, Some(1));
        check_i64(-1, Some(1));
        check_i64(119, Some(1));
        check_i64(-119, Some(1));
        check_i64(120, Some(2));
        check_i64(-120, Some(2));
        check_i64(i64::MAX, Some(9));
        check_i64(i64::MIN, Some(9));
        check_i64(18, None);
        check_i64(189, None);
        check_i64(1834325324, None);
        check_i64(770, None);
        check_i64(-18, None);
        check_i64(-189, None);
        check_i64(-1834325324, None);
        check_i64(-770, None);
        check_i64(0xf010, None);
    }
}