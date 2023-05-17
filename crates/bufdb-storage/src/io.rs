//! Buffer input and output operators.
//! 
//! The buffer is defined with offset. The memory layout as follows:
//! 
//! ``` txt
//! +--------+------------------------+-------------------+
//! | offset |     actual    data     | reserved capacity |
//! +--------+------.-----------------+-------------------+
//! ^        ^      ^                 ^                   |
//! |        |      |<-- available -->|                   |
//! 0        off    pos               len                 |
//! |        |<-------- size -------->|                   |
//! |<------------------- capacity ---------------------->|
//! ```

use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Result;
use std::io::Seek;
use std::io::Write;

use crate::packed_int::PackedI32;
use crate::packed_int::PackedI64;

/// Null strings are UTF encoded as `0xFF`, which is not allowed in a standard UTF encoding.
const UTF_NULL: u8 = 0xff;

/// `Input` trait 
pub trait Input {
    fn read_string(&mut self) -> Result<Option<String>>;
    fn read_u8(&mut self) -> Result<u8>;
    fn read_u16(&mut self) -> Result<u16>;
    fn read_u32(&mut self) -> Result<u32>;
    fn read_u64(&mut self) -> Result<u64>;
    fn read_i8(&mut self) -> Result<i8>;
    fn read_i16(&mut self) -> Result<i16>;
    fn read_i32(&mut self) -> Result<i32>;
    fn read_i64(&mut self) -> Result<i64>;
    fn read_f64(&mut self) -> Result<f64>;
    fn read_packed_i32(&mut self) -> Result<i32>;
    fn read_packed_i64(&mut self) -> Result<i64>;
}

pub trait Output : Sized {
    fn write_string(&mut self, s: Option<&String>) -> Result<()> {
        self.write_str(s.map(|s| s.as_ref()))
    }

    fn write_str(&mut self, s: Option<&str>) -> Result<()>;
    fn write_u8(&mut self, v: u8) -> Result<()>;
    fn write_u16(&mut self, v: u16) -> Result<()>;
    fn write_u32(&mut self, v: u32) -> Result<()>;
    fn write_u64(&mut self, v: u64) -> Result<()>;
    fn write_i8(&mut self, v: i8) -> Result<()>;
    fn write_i16(&mut self, v: i16) -> Result<()>;
    fn write_i32(&mut self, v: i32) -> Result<()>;
    fn write_i64(&mut self, v: i64) -> Result<()>;
    fn write_f64(&mut self, v: f64) -> Result<()>;
    fn write_packed_i32(&mut self, v: i32) -> Result<()>;
    fn write_packed_i64(&mut self, v: i64) -> Result<()>;
}

/// `BufferInput` is a reader for buffer entry.
#[derive(Debug, Clone)]
pub struct BufferInput<'a> {
    data: &'a [u8],
    off: usize,
    len: usize,
    pos: usize
}

impl <'a> BufferInput<'a> {
    /// Creates a reader from entry buffer.
    pub fn new(data: &'a [u8]) -> BufferInput<'a> {
        BufferInput { 
            data, 
            off: 0, 
            len: data.len(), 
            pos: 0 
        }
    }

    /// Creates a reader from entry buffer with offset.
    /// 
    /// `offset` is the starter of the buffer, `size` is the actual length of the data. The full lenth of the buffer should be `offset + size`.
    pub fn new_offset(data: &'a [u8], offset: usize, size: usize) -> BufferInput<'a> {
        if offset + size > data.len() {
            panic!("Index outof bounds.")
        }

        BufferInput { 
            data, 
            off: offset, 
            len: offset + size, 
            pos: offset 
        }
    }

    /// Retrieves the offset of the buffer whitch define the starter of actual data.
    pub fn off(&self) -> usize {
        self.off
    }

    /// Retrieves the full length of the buffer. Including the heading data which is ignored by `offset`.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Retrieves the size of actual data. Excluding the heading data which is ignored by `offset.
    pub fn size(&self) -> usize {
        self.len - self.off
    }

    /// Retrieves the current position. 
    /// 
    /// This position is started from the buffer position `0`, including the `offset` size.
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Retrieves the number of bytes that can be read (orskipped over) from this input buffer.
    pub fn available(&self) -> usize {
        self.len - self.pos
    }

    /// Checks if the End Of File (EOF) is reached.
    pub fn eof(&self) -> bool {
        self.pos >= self.len
    }
}

impl <'a> Read for BufferInput<'a> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.eof() {
            Err(Error::new(ErrorKind::UnexpectedEof, "read end of buffer"))
        } else {
            let count = if buf.len() + self.pos <= self.len {
                buf.copy_from_slice(&self.data[self.pos..self.pos + buf.len()]);
                buf.len()
            } else {
                let len = self.len - self.pos;
                buf[..len].copy_from_slice(&self.data[self.pos..self.len]);
                len
            };
            self.pos = self.pos + count;

            Ok(count)
        }
    }
}

impl <'a> Seek for BufferInput<'a> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64> {
        let position: isize = match pos {
            std::io::SeekFrom::Start(p) => self.off as isize + p as isize,
            std::io::SeekFrom::End(p) => self.len as isize + p as isize,
            std::io::SeekFrom::Current(p) => self.pos as isize + p as isize,
        };

        if position < self.off as _ || position >= self.len as _ {
            Err(Error::new(ErrorKind::InvalidInput, "seek position out of bounds"))
        } else {
            self.pos = position as _;
            Ok(position as _)
        }
    }

    fn rewind(&mut self) -> Result<()> {
        self.pos = self.off;
        Ok(())
    }

    fn stream_position(&mut self) -> Result<u64> {
        Ok(self.pos as _)
    }
}

macro_rules! io_read {
    ($r: expr, $t: ty) => {
        {
            let mut buf = [0u8; std::mem::size_of::<$t>()];
            $r.read(&mut buf)?;
            let v = <$t>::from_be_bytes(buf);
            Ok(v)
        }
    };
}

impl <'a> Input for BufferInput<'a> {
    fn read_string(&mut self) -> Result<Option<String>> {
        if let Some(&n) = self.data.get(self.pos) {
            if n == UTF_NULL {
                self.pos = self.pos + 1;
                Ok(None)
            } else {
                let buffer = &self.data[self.pos..];
                if let Some(p) = buffer.iter().position(|&b| b == 0u8) {
                    match String::from_utf8(buffer[..p].into()) {
                        Ok(s) => {
                            self.pos = self.pos + p + 1;
                            Ok(Some(s))
                        },
                        Err(e) => Err(Error::new(ErrorKind::InvalidData, e.to_string()))
                    }
                } else {
                    Err(Error::new(ErrorKind::InvalidData, "error read string"))
                }
            }
        } else {
            Err(Error::new(ErrorKind::UnexpectedEof, "read string out of bounds"))
        }
    }

    fn read_u8(&mut self) -> Result<u8> {
        io_read!(self, u8)
    }

    fn read_u16(&mut self) -> Result<u16> {
        io_read!(self, u16)
    }

    fn read_u32(&mut self) -> Result<u32> {
        io_read!(self, u32)
    }

    fn read_u64(&mut self) -> Result<u64> {
        io_read!(self, u64)
    }

    fn read_i8(&mut self) -> Result<i8> {
        io_read!(self, i8)
    }

    fn read_i16(&mut self) -> Result<i16> {
        io_read!(self, i16)
    }

    fn read_i32(&mut self) -> Result<i32> {
        io_read!(self, i32)
    }

    fn read_i64(&mut self) -> Result<i64> {
        io_read!(self, i64)
    }

    fn read_f64(&mut self) -> Result<f64> {
        io_read!(self, f64)
    }

    fn read_packed_i32(&mut self) -> Result<i32> {
        let mut v = PackedI32::default();
        let len = v.read(&self.data[self.pos..])?;
        self.pos = self.pos + len;
        Ok(v.into())
    }

    fn read_packed_i64(&mut self) -> Result<i64> {
        let mut v = PackedI64::default();
        let len = v.read(&self.data[self.pos..])?;
        self.pos = self.pos + len;
        Ok(v.into())
    }

}

#[derive(Debug, Clone)]
pub struct BufferOutput {
    data: Vec<u8>,
    off: usize,
    pos: usize
}

impl BufferOutput {
    /// Creates a new `BufferOutput` with default capacity(`64 bytes`).
    pub fn new() -> Self {
        Self {
            data: Vec::with_capacity(64),
            off: 0,
            pos: 0
        }
    }

    /// Creates a new `BufferOutput` with `offset`.
    pub fn new_offset(off: usize, pos: usize) -> Self {
        let mut output = Self::new();
        output.data.resize(off, 0); 
        output.off = off;
        output.pos = pos;
        output
    }

    /// Creates a new `BufferOutput` from a current vec.
    pub fn new_from_vec(data: Vec<u8>, off: usize, pos: usize) -> Self {
        Self {
            data,
            off,
            pos,
        }
    }

    /// Retrieves the offset of the buffer whitch define the starter of actual data.
    pub fn off(&self) -> usize {
        self.off
    }

    /// Retrieves the current position. 
    /// 
    /// This position is started from the buffer position `0`, including the `offset` size.
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Retrieves the full length of the buffer. Including the heading data which is ignored by `offset`.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Retrieves the size of actual data. Excluding the heading data which is ignored by `offset.
    pub fn size(&self) -> usize {
        self.data.len() - self.off
    }

    /// Requires more storage.
    pub fn require(&mut self, need_size: usize) {
        if self.data.len() - self.pos < need_size {
            self.data.resize(self.pos + need_size, 0);
        }
    }
}

impl Default for BufferOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Write for BufferOutput {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if buf.len() <= self.data.len() - self.pos {
            self.data[self.pos..self.pos + buf.len()].copy_from_slice(buf);
        } else {
            let len = self.data.len() - self.pos;
            self.data[self.pos..].copy_from_slice(&buf[..len]);
            self.data.extend_from_slice(&buf[len..]);
        }
        self.pos = self.pos + buf.len();

        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Seek for BufferOutput {
    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64> {
        let pos: isize = match pos {
            std::io::SeekFrom::Start(p) => self.off as isize + p as isize,
            std::io::SeekFrom::End(p) => self.data.len() as isize + p as isize,
            std::io::SeekFrom::Current(p) => self.pos as isize + p as isize,
        };

        if pos < 0 {
            Err(Error::new(ErrorKind::InvalidInput, "seek position out of bounds"))
        } else {
            let pos = pos as usize;
            if pos > self.data.len() {
                self.data.resize(pos, 0);
            }
            self.pos = pos;
            Ok(pos as _)
        }
    }

    fn rewind(&mut self) -> Result<()> {
        self.pos = self.off;
        Ok(())
    }

    fn stream_position(&mut self) -> Result<u64> {
        Ok(self.pos as _)
    }
}

macro_rules! io_write {
    ($w: expr, $v: expr) => {
        {
            let buf = $v.to_be_bytes();
            $w.write(&buf)?;
            Ok(())
        }
    };
}

impl Output for BufferOutput {
    fn write_str(&mut self, s: Option<&str>) -> Result<()> {
        if let Some(s) = s {
            self.write(s.as_bytes())?;
            self.write(&[0u8])?;
        } else {
            self.write(&[UTF_NULL])?;
        }

        Ok(())
    }

    fn write_u8(&mut self, v: u8) -> Result<()> {
        io_write!(self, v)
    }

    fn write_u16(&mut self, v: u16) -> Result<()> {
        io_write!(self, v)
    }

    fn write_u32(&mut self, v: u32) -> Result<()> {
        io_write!(self, v)
    }

    fn write_u64(&mut self, v: u64) -> Result<()> {
        io_write!(self, v)
    }

    fn write_i8(&mut self, v: i8) -> Result<()> {
        io_write!(self, v)
    }

    fn write_i16(&mut self, v: i16) -> Result<()> {
        io_write!(self, v)
    }

    fn write_i32(&mut self, v: i32) -> Result<()> {
        io_write!(self, v)
    }

    fn write_i64(&mut self, v: i64) -> Result<()> {
        io_write!(self, v)
    }

    fn write_f64(&mut self, v: f64) -> Result<()> {
        io_write!(self, v)
    }

    fn write_packed_i32(&mut self, v: i32) -> Result<()> {
        self.require(PackedI32::MAX_LENGETH);
        let val = PackedI32::from(v);
        let len = val.write(&mut self.data[self.pos..])?;
        self.pos = self.pos + len;
        Ok(())
    }

    fn write_packed_i64(&mut self, v: i64) -> Result<()> {
        self.require(PackedI64::MAX_LENGETH);
        let val = PackedI64::from(v);
        let len = val.write(&mut self.data[self.pos..])?;
        self.pos = self.pos + len;
        Ok(())
    }
}