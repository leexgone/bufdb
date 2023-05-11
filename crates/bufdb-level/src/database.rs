use std::path::Path;
use std::path::PathBuf;

use bufdb_api::db_error;
use bufdb_api::error::ErrorKind;
use bufdb_api::error::Result;
use bufdb_storage::entry::BufferEntry;
use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::kv::KV;
use leveldb::options::Options;
use leveldb::options::ReadOptions;
use leveldb_sys::Compression;

use crate::comparator::PKComparator;
use crate::cursor::IDXCursor;
use crate::cursor::PKCursor;

pub struct PrimaryDatabase {
    dir: PathBuf,
    readonly: bool,
    temporary: bool,
    database: Database<BufferEntry>
}

impl PrimaryDatabase {
    pub fn new<C: bufdb_storage::KeyComparator>(dir: PathBuf, readonly: bool, temporary: bool, comparator: C) -> Result<Self> {
        let mut options = Options::new();
        options.create_if_missing = !readonly;
        options.compression = Compression::Snappy;

        let database = match Database::open_with_comparator(&dir, options, PKComparator::from(comparator)) {
            Ok(db) => db,
            Err(e) => return Err(db_error!(open => e)),
        };

        Ok(Self { 
            dir, 
            readonly, 
            temporary, 
            database
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn readonly(&self) -> bool {
        self.readonly
    }

    pub fn temporary(&self) -> bool {
        self.temporary
    }
}

impl bufdb_storage::Database<PKCursor> for PrimaryDatabase {
    fn count(&self) -> bufdb_api::error::Result<usize> {
        let options = ReadOptions::new();
        let count = self.database.iter(options).count();
        Ok(count)
    }

    fn put(&mut self, key: &bufdb_storage::entry::BufferEntry, data: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<()> {
        todo!()
    }

    fn get(&self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<Option<bufdb_storage::entry::BufferEntry>> {
        todo!()
    }

    fn delete(&mut self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn open_cursor(&self) -> bufdb_api::error::Result<PKCursor> {
        todo!()
    }
}

#[derive(Debug)]
pub struct SecondaryDatabase {

}

impl bufdb_storage::Database<IDXCursor> for SecondaryDatabase {
    fn count(&self) -> bufdb_api::error::Result<usize> {
        todo!()
    }

    fn put(&mut self, key: &bufdb_storage::entry::BufferEntry, data: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<()> {
        todo!()
    }

    fn get(&self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<Option<bufdb_storage::entry::BufferEntry>> {
        todo!()
    }

    fn delete(&mut self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn open_cursor(&self) -> bufdb_api::error::Result<IDXCursor> {
        todo!()
    }
}