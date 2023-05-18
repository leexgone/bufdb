use std::cmp::Ordering;

use bufdb_api::error::ErrorKind;
use bufdb_api::error::Result;
use db_key::Key;

use crate::io::BufferInput;
use crate::io::BufferOutput;

pub trait Entry : AsRef<[u8]> {
    fn off(&self) -> usize;

    fn len(&self) -> usize;

    fn size(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.off() == self.len()
    }

    fn slice(&self) -> &[u8];

    fn as_input(&self) -> BufferInput {
        BufferInput::new(self.slice())
    }

    fn left(&self, n: usize) -> Result<SliceEntry> {
        if n > self.size() {
            Err(ErrorKind::OutOfBounds.into())
        } else {
            Ok(SliceEntry::new_off(self.as_ref(), self.off(), n))
        }
    }
}

pub fn compare<K1: Entry, K2: Entry>(key1: &K1, key2: &K2) -> Ordering {
    let data1 = key1.slice();
    let data2 = key2.slice();

    let mut iter1 = data1.iter();
    for v2 in data2.iter() {
        if let Some(v1) = iter1.next() {
            let c = v1.cmp(v2);
            if !c.is_eq() {
                return c;
            }
        } else {
            return Ordering::Less;
        }
    }

    if iter1.next().is_none() {
        Ordering::Equal
    } else {
        Ordering::Greater
    }
}

#[derive(Debug, Default, Clone, Eq, Ord)]
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

    pub fn set_buffer(&mut self, buffer: BufferEntry) {
        self.data = buffer.data;
        self.off = buffer.off;
        self.len = buffer.len;
    }

    pub fn set_len(&mut self, len: usize) {
        if len > self.data.len() {
            self.data.resize(len, 0);
        }

        self.len = len;
    }

    pub fn as_slice_entry(&self) -> SliceEntry {
        SliceEntry::new_off(self.as_ref(), self.off, self.size())
    }
}

impl Entry for BufferEntry {
    fn off(&self) -> usize {
        self.off
    }

    fn len(&self) -> usize {
        self.len
    }

    fn size(&self) -> usize {
        self.len - self.off
    }

    fn slice(&self) -> &[u8] {
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
        BufferOutput::new_from_vec(self.data, self.off, self.off)
    }
}


impl Key for BufferEntry {
    fn from_u8(key: &[u8]) -> Self {
        let data = Vec::from(key);
        data.into()
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(self.slice())
    }
}
impl PartialEq for BufferEntry {
    fn eq(&self, other: &Self) -> bool {
        if self.size() == other.size() {
            compare(self, other) == Ordering::Equal
        } else {
            false
        }
    }
}

impl PartialOrd for BufferEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(compare(self, other))
    }
}

#[derive(Debug, Clone, Eq, Ord)]
pub struct SliceEntry<'a> {
    data: &'a [u8],
    off: usize,
    len: usize
}

impl <'a> SliceEntry<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        let len = data.len();
        Self { 
            data, 
            off: 0, 
            len
        }
    }

    pub fn new_off(data: &'a [u8], off: usize, size: usize) -> Self {
        Self { 
            data, 
            off, 
            len: off + size 
        }
    }
}

impl <'a> Entry for SliceEntry<'a> {
    fn off(&self) -> usize {
        self.off
    }

    fn len(&self) -> usize {
        self.len
    }

    fn size(&self) -> usize {
        self.len - self.off
    }

    fn slice(&self) -> &[u8] {
        &self.data[self.off..self.len]
    }

    fn as_input(&self) -> BufferInput {
        BufferInput::new_offset(self.data, self.off, self.len)
    }
}

impl <'a> AsRef<[u8]> for SliceEntry<'a> {
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}

impl <'a> Into<BufferInput<'a>> for &'a SliceEntry<'a> {
    fn into(self) -> BufferInput<'a> {
        self.as_input()
    }
}

impl <'a> PartialEq for SliceEntry<'a> {
    fn eq(&self, other: &Self) -> bool {
        if self.size() == other.size() {
            compare(self, other) == Ordering::Equal
        } else {
            false
        }
    }
}

impl <'a> PartialOrd for SliceEntry<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(compare(self, other))
    }
}