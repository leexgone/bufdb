use std::path::Path;
use std::path::PathBuf;

use bufdb_api::error::Result;
use bufdb_storage::Environment;
use bufdb_storage::entry::BufferEntry;
use leveldb::database::Database;
use leveldb::options::Options;
use leveldb_sys::Compression;

use crate::cursor::IDXCursor;
use crate::cursor::PKCursor;
use crate::database::PrimaryDatabase;
use crate::database::SecondaryDatabase;

pub struct LevelDBEnv {
    dir: PathBuf,
    readonly: bool,
    temporary: bool,
}

impl LevelDBEnv {
    pub fn new(dir: PathBuf, readonly: bool, temporary: bool) -> Result<Self> {
        Ok(Self {
            dir,
            readonly,
            temporary
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

    fn get_data_dir(&self, name: &str) -> PathBuf {
        let mut dir = self.dir.clone();
        dir.push(Path::new(name));
        dir
    }
}

impl Environment for LevelDBEnv {
    type CURSOR = PKCursor;
    type SCUROSR = IDXCursor;
    type DATABASE = PrimaryDatabase;
    type SDATABASE = SecondaryDatabase;

    fn create_database<C: bufdb_storage::KeyComparator>(&mut self, name: &str, config: bufdb_api::config::TableConfig, comparator: C) -> bufdb_api::error::Result<Self::DATABASE> {
        let data_dir = self.get_data_dir(name);

        PrimaryDatabase::new(data_dir, config.readonly, config.temporary, comparator)
    }

    fn create_secondary_database<C: bufdb_storage::KeyComparator>(&mut self, database: &Self::DATABASE, name: &str, define: bufdb_api::model::IndexDefine, comparator: C) -> bufdb_api::error::Result<Self::SDATABASE> {
        todo!()
    }

    fn drop_database(&mut self, name: &str) -> bufdb_api::error::Result<()> {
        todo!()
    }

    fn drop_secondary_database(&mut self, name: &str) -> bufdb_api::error::Result<()> {
        todo!()
    }

    fn truncate_database(&mut self, name: &str) -> bufdb_api::error::Result<()> {
        todo!()
    }

    fn rename_database(&mut self, raw_name: &str, new_name: &str) -> bufdb_api::error::Result<()> {
        todo!()
    }
}