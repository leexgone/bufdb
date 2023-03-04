use crate::error::Result;

#[derive(Debug, Default)]
pub struct BufferEntry {
    data: Vec<u8>,
    offset: usize,
    len: usize
}

impl BufferEntry {
    pub fn new(data: Vec<u8>, offset: usize, len: usize) -> BufferEntry {
        BufferEntry { data, offset, len }
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn set_data(&mut self, data: Vec<u8>) {
        self.offset = 0;
        self.len = data.len();
        self.data = data;
    }

    pub fn set_data_offset(&mut self, data: Vec<u8>, offset: usize, len: usize) {
        self.offset = offset;
        self.len = len;
        self.data = data;
    }
}

impl AsRef<Vec<u8>> for BufferEntry {
    fn as_ref(&self) -> &Vec<u8> {
        &self.data
    }
}

impl AsMut<Vec<u8>> for BufferEntry {
    fn as_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
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
            offset: 0, 
            len
        }
    }
}

impl From<&[u8]> for BufferEntry {
    fn from(data: &[u8]) -> Self {
        BufferEntry { 
            data: data.into(), 
            offset: 0, 
            len: data.len() 
        }
    }
}

impl<const N: usize> From<[u8; N]> for BufferEntry  {
    fn from(data: [u8; N]) -> Self {
        BufferEntry { 
            data: data.into(), 
            offset: 0, 
            len: N 
        }
    }
}

pub trait Database {
    fn count(&self) -> Result<usize>;
    fn put(&self, key: &BufferEntry, data: &BufferEntry) -> Result<()>;
    fn get(&mut self, key: &BufferEntry) -> Result<Option<BufferEntry>>;
    fn delete(&mut self, key: &BufferEntry) -> Result<bool>;
}