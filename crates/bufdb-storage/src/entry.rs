use db_key::Key;

use crate::io::BufferInput;
use crate::io::BufferOutput;

#[derive(Debug, Default, Clone)]
pub struct BufferEntry {
    data: Vec<u8>,
    off: usize,
    len: usize
}

impl BufferEntry {
    pub fn new<T: Into<Vec<u8>>>(data: T, off: usize, size: usize) -> BufferEntry {
        BufferEntry { 
            data: data.into(), 
            off, 
            len: off + size
        }
    }

    pub fn off(&self) -> usize {
        self.off
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn size(&self) -> usize {
        self.len - self.off
    }

    pub fn set_data(&mut self, data: Vec<u8>) {
        self.off = 0;
        self.len = data.len();
        self.data = data;
    }

    pub fn set_data_offset(&mut self, data: Vec<u8>, off: usize, size: usize) {
        self.off = off;
        self.len = off + size;
        self.data = data;
    }

    pub fn slice(&self) -> &[u8] {
        &self.data[self.off..self.len]
    }
}

impl AsRef<Vec<u8>> for BufferEntry {
    fn as_ref(&self) -> &Vec<u8> {
        &self.data
    }
}

impl AsRef<[u8]> for BufferEntry {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsMut<[u8]> for BufferEntry {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl From<Vec<u8>> for BufferEntry {
    fn from(data: Vec<u8>) -> Self {
        let len = data.len();
        BufferEntry { 
            data, 
            off: 0, 
            len
        }
    }
}

impl Into<Vec<u8>> for BufferEntry {
    fn into(self) -> Vec<u8> {
        self.data
    }
}

impl <'a> Into<BufferInput<'a>> for &'a BufferEntry {
    fn into(self) -> BufferInput<'a> {
        BufferInput::new_offset(self.as_ref(), self.off, self.size())
    }
}

impl Into<BufferOutput> for BufferEntry {
    fn into(self) -> BufferOutput {
        BufferOutput::new_from_vec(self.data, self.off)
    }
}

impl Key for BufferEntry {
    fn from_u8(key: &[u8]) -> Self {
        let data = Vec::from(key);
        data.into()
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        let key = &self.data[self.off..self.len];
        f(key)
    }
}
