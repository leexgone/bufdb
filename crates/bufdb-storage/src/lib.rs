use std::cmp::Ordering;

use bufdb_api::config::TableConfig;
use bufdb_api::error::Result;
use bufdb_api::model::IndexDefine;
use entry::BufferEntry;

pub mod entry;
pub mod io;
pub(crate) mod packed_int;

pub trait Database<C: Cursor> {
    fn count(&self) -> Result<usize>;
    fn put(&self, key: &BufferEntry, data: &BufferEntry) -> Result<()>;
    fn get(&self, key: &BufferEntry) -> Result<Option<BufferEntry>>;
    fn delete(&self, key: &BufferEntry) -> Result<()>;
    fn delete_exist(&self, key: &BufferEntry) -> Result<bool>;
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

    fn create_database<C: KeyComparator>(&mut self, name: &str, config: DatabaseConfig<C>) -> Result<Self::DATABASE>;
    fn create_secondary_database<C: KeyComparator, G: KeyCreator>(&mut self, database: &Self::DATABASE, name: &str, config: SDatabaseConfig<C, G>) -> Result<Self::SDATABASE>;
    fn drop_database(&mut self, name: &str) -> Result<()>;
    fn drop_secondary_database(&mut self, name: &str) -> Result<()>;
    fn truncate_database(&mut self, name: &str) -> Result<()>;
    fn rename_database(&mut self, raw_name: &str, new_name: &str) -> Result<()>;
}

pub trait KeyComparator {
    fn compare(&self, key1: &BufferEntry, key2: &BufferEntry) -> Result<Ordering>;
}

pub trait KeyCreator {
    fn create_key(&self, key: &BufferEntry, data: &BufferEntry) -> Result<Option<BufferEntry>>;
}

pub struct DatabaseConfig<C: KeyComparator> {
    pub readonly: bool,
    pub temporary: bool,
    pub comparator: C
}

pub struct SDatabaseConfig<C: KeyComparator, G: KeyCreator> {
    pub readonly: bool,
    pub temporary: bool,
    pub comparator: C,
    pub creator: G
}