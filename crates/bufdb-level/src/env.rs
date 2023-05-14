use std::path::Path;
use std::path::PathBuf;

use bufdb_api::error::Result;
use bufdb_storage::DatabaseConfig;
use bufdb_storage::Environment;
use bufdb_storage::KeyComparator;
use bufdb_storage::KeyCreator;
use bufdb_storage::SDatabaseConfig;

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

    fn create_database<C: KeyComparator>(&mut self, name: &str, config: DatabaseConfig<C>) -> bufdb_api::error::Result<Self::DATABASE> {
        let data_dir = self.get_data_dir(name);

        PrimaryDatabase::new(name, data_dir, config.readonly, config.temporary, config.comparator)
    }

    fn create_secondary_database<C: KeyComparator, G: KeyCreator>(&mut self, database: &Self::DATABASE, name: &str, config: SDatabaseConfig<C, G>) -> bufdb_api::error::Result<Self::SDATABASE> {
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