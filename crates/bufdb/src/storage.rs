use crate::error::Result;
use crate::model::IndexDefine;
use crate::model::TableDefine;

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

pub trait Database<C: Cursor> {
    fn count(&self) -> Result<usize>;
    fn put(&mut self, key: &BufferEntry, data: &BufferEntry) -> Result<()>;
    fn get(&self, key: &BufferEntry) -> Result<Option<BufferEntry>>;
    fn delete(&mut self, key: &BufferEntry) -> Result<bool>;
    fn open_cursor(&self) -> Result<C>;
}

pub trait Cursor {
    fn search(&self, key: &BufferEntry, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn search_range(&self, key: &mut BufferEntry, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn next(&self, key: &mut BufferEntry, data: &mut BufferEntry) -> Result<bool>;
    fn next_dup(&self, key: &mut BufferEntry, data: &mut BufferEntry) -> Result<bool>;
    fn skip(&self, count: usize, key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn delete(&mut self, key: &BufferEntry) -> Result<bool>;
    fn update(&mut self, key: &BufferEntry, data: &BufferEntry) -> Result<bool>;
}

pub trait SecondaryCursor : Cursor {
    fn s_search(&self, key: &BufferEntry, p_key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn s_search_range(&self, key: &mut BufferEntry, p_key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn s_next(&self, key: &mut BufferEntry, p_key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn s_next_dup(&self, key: &mut BufferEntry, p_key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
}

pub trait Environment {
    type CURSOR: Cursor;
    type SCUROSR: SecondaryCursor;
    type DATABASE: Database<Self::CURSOR>;
    type SDATABASE: Database<Self::SCUROSR>;

    fn create_database<C: KeyComparator>(&mut self, define: TableDefine, config: TableConfig, comparator: C) -> Result<Self::DATABASE>;
    fn create_secondary_database<C: KeyComparator>(&mut self, database: &Self::DATABASE, name: &str, define: IndexDefine, comparator: C) -> Result<Self::SDATABASE>;
    fn drop_database(&mut self, name: &str) -> Result<()>;
    fn drop_secondary_database(&mut self, name: &str) -> Result<()>;
    fn truncate_database(&mut self, name: &str) -> Result<()>;
    fn rename_database(&mut self, raw_name: &str, new_name: &str) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct TableConfig {

}

pub trait KeyComparator {
    fn compare(&self, key1: &BufferEntry, key2: &BufferEntry) -> Result<i32>;

    fn find_shortest_separator<'a>(&self, start: &'a BufferEntry, _limit: &'a BufferEntry) -> Result<&'a BufferEntry> {
        Ok(start)
    }

    fn fins_shortest_successor<'a>(&self, key: &'a BufferEntry) -> Result<&'a BufferEntry> {
        Ok(key)
    }
}