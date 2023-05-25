use std::cmp::Ordering;
use std::fmt::Debug;
use std::path::PathBuf;

use bufdb_api::error::Result;
use entry::BufferEntry;
use entry::Entry;

pub mod entry;
pub mod io;
pub(crate) mod packed_int;

pub trait PrimaryCursor<'a> {
    fn search(&mut self, key: &BufferEntry, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn search_range(&mut self, key: &mut BufferEntry, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn next(&mut self, key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn next_dup(&mut self, key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn skip(&mut self, count: usize, key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
}

pub trait SecondaryCursor<'a> : PrimaryCursor<'a> {
    fn s_search(&mut self, key: &BufferEntry, p_key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn s_search_range(&mut self, key: &mut BufferEntry, p_key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn s_next(&mut self, key: Option<&mut BufferEntry>, p_key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn s_next_dup(&mut self, key: Option<&mut BufferEntry>, p_key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
    fn s_skip(&mut self, count: usize, key: Option<&mut BufferEntry>, p_key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<bool>;
}

pub trait Database<'a, C: PrimaryCursor<'a>> {
    fn count(&self) -> Result<usize>;
    fn put(&self, key: &BufferEntry, data: &BufferEntry) -> Result<()>;
    fn get(&self, key: &BufferEntry) -> Result<Option<BufferEntry>>;
    fn delete(&self, key: &BufferEntry) -> Result<()>;
    fn delete_exist(&self, key: &BufferEntry) -> Result<bool>;
    fn open_cursor(&'a self) -> Result<C>;
}

pub trait Environment<'a> : Sized {
    type CURSOR: PrimaryCursor<'a>;
    type SCUROSR: SecondaryCursor<'a>;
    type DATABASE: Database<'a, Self::CURSOR>;
    type SDATABASE: Database<'a, Self::SCUROSR>;

    fn new(config: EnvironmentConfig) -> Result<Self>;
    fn create_database<C: KeyComparator>(&mut self, name: &str, config: DatabaseConfig<C>) -> Result<Self::DATABASE>;
    fn create_secondary_database<C: KeyComparator, G: KeyCreator + 'a>(&mut self, database: &Self::DATABASE, name: &str, config: SDatabaseConfig<C, G>) -> Result<Self::SDATABASE>;
    fn drop_database(&mut self, name: &str) -> Result<()>;
    fn drop_secondary_database(&mut self, name: &str) -> Result<()>;
    fn truncate_database(&mut self, name: &str) -> Result<()>;
    fn rename_database(&mut self, raw_name: &str, new_name: &str) -> Result<()>;
}

pub trait KeyComparator : Debug {
    fn compare<T: Entry>(&self, key1: &T, key2: &T) -> Result<Ordering>;
}

pub trait KeyCreator : Debug {
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
    pub unique: bool,
    pub comparator: C,
    pub creator: G
}

pub struct EnvironmentConfig {
    pub dir: PathBuf,
    pub readonly: bool,
    pub temporary: bool,
}

pub trait StorageEngine<'a> : Copy + Clone {
    type CURSOR: PrimaryCursor<'a>;
    type SCUROSR: SecondaryCursor<'a>;
    type DATABASE: Database<'a, Self::CURSOR>;
    type SDATABASE: Database<'a, Self::SCUROSR>;
    type ENVIRONMENT: Environment<'a>;

    fn name(&self) -> &str;
}
