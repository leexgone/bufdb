use std::sync::atomic::AtomicI64;

use bufdb_api::config::TableConfig;
use bufdb_api::error::Result;
use bufdb_storage::DatabaseConfig;
use bufdb_storage::Environment;
use bufdb_storage::KeyComparator;
use bufdb_storage::StorageEngine;
use bufdb_storage::cache::Poolable;
use bufdb_storage::cache::now;
use bufdb_storage::get_timestamp;
use bufdb_storage::set_timestamp;

use crate::daemon::Maintainable;

pub(crate) struct TableImpl<'a, T: StorageEngine<'a>> {
    name: String,
    config: TableConfig,
    db: <<T as StorageEngine<'a>>::ENVIRONMENT as Environment<'a>>::DATABASE,
    last_access: AtomicI64,
}

impl <'a, T: StorageEngine<'a>> TableImpl<'a, T> {
    pub fn new<S: Into<String>, C: KeyComparator>(env: &T::ENVIRONMENT, name: S, config: TableConfig, comparator: C) -> Result<Self> {
        let name: String = name.into();

        let db_config = DatabaseConfig {
            readonly: config.readonly,
            temporary: config.temporary,
            comparator
        };
        let db = env.create_database(&name, db_config)?;
        
        Ok(Self { 
            name, 
            config, 
            db, 
            last_access: AtomicI64::new(now()) 
        })
    }

    pub fn config(&self) -> &TableConfig {
        &self.config
    }
}

unsafe impl <'a, T: StorageEngine<'a>> Send for TableImpl<'a, T> {}
unsafe impl <'a, T: StorageEngine<'a>> Sync for TableImpl<'a, T> {}

impl <'a, T: StorageEngine<'a>> Maintainable for TableImpl<'a, T> {
    fn maintain(&self) {
        todo!()
    }
}

impl <'a, T: StorageEngine<'a>> Poolable for TableImpl<'a, T> {
    fn name(&self) -> &str {
        &self.name
    }

    fn last_access(&self) -> i64 {
        get_timestamp!(self.last_access)
    }

    fn touch(&self) {
        set_timestamp!(self.last_access)
    }
}