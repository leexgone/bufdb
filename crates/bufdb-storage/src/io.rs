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

use crate::entry::BufferEntry;
use crate::entry::Entry;
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

pub trait Inputable : Sized {
    fn read_from<R: Input>(reader: &mut R) -> Result<Self>;

    fn from_entry<T: Entry>(entry: &T) -> Result<Self> {
        let mut input = entry.as_input();
        Self::read_from(&mut input)
    }
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

pub trait Outputable {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()>;

    fn to_entry(&self) -> Result<BufferEntry> {
        let mut output = BufferOutput::new();
        self.write_to(&mut output)?;
        Ok(output.into())
    }
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

impl Inputable for Option<String> {
    fn read_from<R: Input>(reader: &mut R) -> Result<Self> {
        reader.read_string()
    }
}

impl Inputable for String {
    fn read_from<R: Input>(reader: &mut R) -> Result<Self> {
        let s = reader.read_string()?;
        Ok(s.unwrap_or_default())
    }
}

impl Inputable for u8 {
    fn read_from<R: Input>(reader: &mut R) -> Result<Self> {
        reader.read_u8()
    }
}

impl Inputable for u16 {
    fn read_from<R: Input>(reader: &mut R) -> Result<Self> {
        reader.read_u16()
    }
}

impl Inputable for u32 {
    fn read_from<R: Input>(reader: &mut R) -> Result<Self> {
        reader.read_u32()
    }
}

impl Inputable for u64 {
    fn read_from<R: Input>(reader: &mut R) -> Result<Self> {
        reader.read_u64()
    }
}

impl Inputable for i8 {
    fn read_from<R: Input>(reader: &mut R) -> Result<Self> {
        reader.read_i8()
    }
}

impl Inputable for i16 {
    fn read_from<R: Input>(reader: &mut R) -> Result<Self> {
        reader.read_i16()
    }
}

impl Inputable for i32 {
    fn read_from<R: Input>(reader: &mut R) -> Result<Self> {
        reader.read_i32()
    }
}

impl Inputable for i64 {
    fn read_from<R: Input>(reader: &mut R) -> Result<Self> {
        reader.read_i64()
    }
}

impl Inputable for f64 {
    fn read_from<R: Input>(reader: &mut R) -> Result<Self> {
        reader.read_f64()
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
        self.pos - self.off
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

impl Outputable for Option<&str> {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()> {
        writer.write_str(*self)
    }
}

impl Outputable for String {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()> {
        writer.write_str(Some(self.as_str()))
    }
}

impl Outputable for str {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()> {
        writer.write_str(Some(self))
    }
}

impl Outputable for u8 {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()> {
        writer.write_u8(*self)
    }
}

impl Outputable for u16 {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()> {
        writer.write_u16(*self)
    }
}

impl Outputable for u32 {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()> {
        writer.write_u32(*self)
    }
}

impl Outputable for u64 {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()> {
        writer.write_u64(*self)
    }
}

impl Outputable for i8 {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()> {
        writer.write_i8(*self)
    }
}

impl Outputable for i16 {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()> {
        writer.write_i16(*self)
    }
}

impl Outputable for i32 {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()> {
        writer.write_i32(*self)
    }
}

impl Outputable for i64 {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()> {
        writer.write_i64(*self)
    }
}

impl Outputable for f64 {
    fn write_to<W: Output>(&self, writer: &mut W) -> Result<()> {
        writer.write_f64(*self)
    }
}

impl Into<BufferEntry> for BufferOutput {
    fn into(self) -> BufferEntry {
        let size = self.size();
        BufferEntry::new(self.data, self.off, size)
    }
}

#[cfg(test)]
mod tests {
    use crate::entry::BufferEntry;
    use crate::io::Input;

    use super::BufferInput;
    use super::BufferOutput;
    use super::Output;

    #[test]
    fn test_io() {
        let mut output = BufferOutput::new();

        output.write_str(None).unwrap();
        output.write_str(Some("Hello")).unwrap();
        output.write_u8(123u8).unwrap();
        output.write_u16(12345u16).unwrap();
        output.write_u32(1234567u32).unwrap();
        output.write_u64(1234567890u64).unwrap();
        output.write_i8(-123i8).unwrap();
        output.write_i16(-12345i16).unwrap();
        output.write_i32(-1234567i32).unwrap();
        output.write_i64(-1234567890i64).unwrap();
        output.write_f64(1234567.89f64).unwrap();
        output.write_packed_i32(7654321i32).unwrap();
        output.write_packed_i64(987654321i64).unwrap();

        let buffer: BufferEntry = output.into();

        let mut input: BufferInput = (&buffer).into();

        assert_eq!(None, input.read_string().unwrap());
        assert_eq!(Some(String::from("Hello")), input.read_string().unwrap());
        assert_eq!(123u8, input.read_u8().unwrap());
        assert_eq!(12345u16, input.read_u16().unwrap());
        assert_eq!(1234567u32, input.read_u32().unwrap());
        assert_eq!(1234567890u64, input.read_u64().unwrap());
        assert_eq!(-123i8, input.read_i8().unwrap());
        assert_eq!(-12345i16, input.read_i16().unwrap());
        assert_eq!(-1234567i32, input.read_i32().unwrap());
        assert_eq!(-1234567890i64, input.read_i64().unwrap());
        assert_eq!(1234567.89f64, input.read_f64().unwrap());
        assert_eq!(7654321i32, input.read_packed_i32().unwrap());
        assert_eq!(987654321i64, input.read_packed_i64().unwrap());

        assert!(input.eof());
    }
}