use std::io::ErrorKind;
use std::io::Read;
use std::io::Seek;
use std::iter;

#[derive(Debug, Clone)]
pub struct BufferInput<'a> {
    data: &'a [u8],
    off: usize,
    size: usize,
    pos: usize
}

impl <'a> BufferInput<'a> {
    pub fn new(data: &'a [u8]) -> BufferInput<'a> {
        BufferInput { 
            data, 
            off: 0, 
            size: data.len(), 
            pos: 0 
        }
    }

    pub fn new_offset(data: &'a [u8], offset: usize, length: usize) -> BufferInput<'a> {
        if offset + length > data.len() {
            panic!("Index outof bounds.")
        }

        BufferInput { 
            data, 
            off: offset, 
            size: offset + length, 
            pos: offset 
        }
    }

    pub fn offset(&self) -> usize {
        self.off
    }

    pub fn len(&self) -> usize {
        self.size - self.off
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn available(&self) -> usize {
        self.size - self.pos
    }

    pub fn eof(&self) -> bool {
        self.pos >= self.size
    }
}

impl <'a> Read for BufferInput<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.eof() {
            Err(ErrorKind::UnexpectedEof.into())
        } else {
            let count = if buf.len() + self.pos <= self.size {
                buf.copy_from_slice(&self.data[self.pos..self.pos + buf.len()]);
                buf.len()
            } else {
                let len = self.size - self.pos;
                buf[..len].copy_from_slice(&self.data[self.pos..self.size]);
                len
            };
            self.pos = self.pos + count;

            Ok(count)
        }
    }
}

impl <'a> Seek for BufferInput<'a> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let position = match pos {
            std::io::SeekFrom::Start(p) => self.off + p as usize,
            std::io::SeekFrom::End(p) => self.size - p as usize,
            std::io::SeekFrom::Current(p) => self.pos + p as usize,
        };

        if position < self.off || position >= self.size {
            Err(ErrorKind::InvalidInput.into())
        } else {
            self.pos = position;
            Ok(position as u64)
        }
    }

    fn rewind(&mut self) -> std::io::Result<()> {
        self.pos = self.off;
        Ok(())
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        Ok(self.pos as _)
    }
}

#[derive(Debug, Clone, Default)]
pub struct BufferOutput {
    data: Vec<u8>,
    off: usize,
    pos: usize
}

impl BufferOutput {
    pub fn new() -> Self {
        Self {
            data: Vec::with_capacity(64),
            off: 0,
            pos: 0
        }
    }

    pub fn new_offset(offset: usize) -> Self {
        let mut output = Self::new();
        output.data.extend(iter::repeat(0).take(offset));
        output.off = offset;
        output
    }

    pub fn offset(&self) -> usize {
        self.off
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}