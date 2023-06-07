use std::fs::remove_dir_all;
use std::fs::rename;
use std::path::Path;
use std::path::PathBuf;

use bufdb_api::error::Result;
use bufdb_storage::DatabaseConfig;
use bufdb_storage::Environment;
use bufdb_storage::EnvironmentConfig;
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

    fn clear_database(&self, name: &str) -> Result<()> {
        let dir = self.get_data_dir(name);
        remove_dir_all(dir)?;
        Ok(())
    }
}

impl <'a> Environment<'a> for LevelDBEnv {
    type CURSOR = PKCursor<'a>;
    type SCUROSR = IDXCursor<'a>;
    type DATABASE = PrimaryDatabase<'a>;
    type SDATABASE = SecondaryDatabase<'a>;

    fn new(config: EnvironmentConfig) -> Result<Self> {
        Ok(Self {
            dir: config.dir,
            readonly: config.readonly,
            temporary: config.temporary,
        })
    }

    fn create_database<C: KeyComparator>(&self, name: &str, config: DatabaseConfig<C>) -> bufdb_api::error::Result<Self::DATABASE> {
        let data_dir = self.get_data_dir(name);

        PrimaryDatabase::new(name, data_dir, config.readonly, config.temporary, config.comparator)
    }

    fn create_secondary_database<C: KeyComparator, G: KeyCreator + 'a>(&self, database: &Self::DATABASE, name: &str, config: SDatabaseConfig<C, G>) -> bufdb_api::error::Result<Self::SDATABASE> {
        SecondaryDatabase::new(database, name, config)
    }

    fn drop_database(&self, name: &str) -> bufdb_api::error::Result<()> {
        self.clear_database(name)
    }

    fn drop_secondary_database(&self, name: &str) -> bufdb_api::error::Result<()> {
        self.clear_database(name)
    }

    fn truncate_database(&self, name: &str) -> bufdb_api::error::Result<()> {
        self.clear_database(name)
    }

    fn rename_database(&self, raw_name: &str, new_name: &str) -> bufdb_api::error::Result<()> {
        let raw_dir = self.get_data_dir(raw_name);
        let new_dir = self.get_data_dir(new_name);
        rename(raw_dir, new_dir)?;
        Ok(())
    }
}