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

/// `Input` trait 
pub trait Input {
    fn read_string(&self) -> Result<String>;
    fn read_u8(&self) -> Result<u8>;
    fn read_u16(&self) -> Result<u16>;
    fn read_u32(&self) -> Result<u32>;
    fn read_u64(&self) -> Result<u64>;
    fn read_i8(&self) -> Result<i8>;
    fn read_i16(&self) -> Result<i16>;
    fn read_i32(&self) -> Result<i32>;
    fn read_i64(&self) -> Result<i64>;
    fn read_f64(&self) -> Result<f64>;
    fn read_packed_i32(&self) -> Result<i32>;
    fn read_packed_i64(&self) -> Result<i64>;
}

pub trait Output : Sized {
    fn write_string(&mut self, s: &String) -> Result<()> {
        self.write_str(s.as_str())
    }

    fn write_str(&mut self, s: &str) -> Result<()>;
    fn write_u8(&mut self) -> Result<()>;
    fn write_u16(&mut self) -> Result<()>;
    fn write_u32(&mut self) -> Result<()>;
    fn write_u64(&mut self) -> Result<()>;
    fn write_i8(&mut self) -> Result<()>;
    fn write_i16(&mut self) -> Result<()>;
    fn write_i32(&mut self) -> Result<()>;
    fn write_i64(&mut self) -> Result<()>;
    fn write_f64(&mut self) -> Result<()>;
    fn write_packed_i32(&mut self) -> Result<()>;
    fn write_packed_i64(&mut self) -> Result<()>;
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
    pub fn new_offset(offset: usize) -> Self {
        let mut output = Self::new();
        output.data.resize(offset, 0); 
        output.off = offset;
        output
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
