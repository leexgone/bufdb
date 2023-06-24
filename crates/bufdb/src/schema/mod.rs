use std::fmt::Display;
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use bufdb_api::config::CacheConfig;
use bufdb_api::config::SchemaConfig;
use bufdb_api::config::TableConfig;
use bufdb_api::error::ErrorKind;
use bufdb_api::error::Result;
use bufdb_api::model::TableDefine;
use bufdb_storage::DatabaseConfig;
use bufdb_storage::Environment;
use bufdb_storage::EnvironmentConfig;
use bufdb_storage::KeyComparator;
use bufdb_storage::StorageEngine;
use bufdb_storage::cache::CachePool;
use bufdb_storage::cache::Poolable;
use bufdb_storage::cache::now;
use bufdb_storage::get_timestamp;
use bufdb_storage::set_timestamp;

use crate::daemon::Maintainable;
use crate::engine::DBEngine;
use crate::instance::InstImpl;
use crate::table::KVTable;
use crate::table::StringKeyComparator;
use crate::table::TableImpl;

use self::meta::MetaStorage;

mod meta;

pub(crate) struct SchemaImpl<'a, T: StorageEngine<'a>> {
    name: String,
    config: SchemaConfig,
    env: T::ENVIRONMENT,
    meta: MetaStorage<'a, T>, 
    tables: CachePool<TableImpl<'a, T>>,
    last_access: AtomicI64,
}

impl <'a, T: StorageEngine<'a>> SchemaImpl<'a, T> {
    pub fn new<S: Into<String>>(inst_dir: &Path, name: S, config: SchemaConfig) -> Result<Self> {
        let name: String = name.into();

        let dir = inst_dir.join(&name);
        if !dir.is_dir() {
            create_dir_all(&dir)?;
        }

        let env_config = EnvironmentConfig {
            dir,
            readonly: config.readonly(),
            temporary: config.temporary(),
        };
        let env = T::ENVIRONMENT::new(env_config)?;

        let db_config = DatabaseConfig {
            readonly: config.readonly(),
            temporary: config.temporary(),
            comparator: StringKeyComparator {},
        };
        let meta = env.create_database("SYS_META", db_config)?;

        Ok(Self { 
            name, 
            config, 
            env, 
            meta: MetaStorage { db: Some(meta) },
            tables: CachePool::new(), 
            last_access: AtomicI64::new(now()),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn config(&self) -> &SchemaConfig {
        &self.config
    }

    fn open<C: KeyComparator>(&self, name: &str, config: TableConfig, comparator: C) -> Result<Arc<TableImpl<'a, T>>> {
        self.touch();
        
        if let Some(table) = self.tables.get(name) {
            if table.config().readonly() != config.readonly() || table.config().temporary() != config.temporary() {
                Err(ErrorKind::Configuration.into())
            } else {
                Ok(table)
            }
        } else {
            let table = TableImpl::new(&self.env, name, config, comparator)?;
            let table = Arc::new(table);
            if self.config.max_cache().is_some() {
                self.tables.put(table.clone());
            }
            Ok(table)
        }
    }

    pub fn create_kv_table(&self, name: &str, config: TableConfig) -> Result<Arc<TableImpl<'a, T>>> {
        if config.temporary() && config.readonly() {
            Err(ErrorKind::Configuration.into())
        } else if self.meta.exists(name)? {
            Err(ErrorKind::CreateDuplicate.into())
        } else {
            let table = self.open(name, config, StringKeyComparator {})?;
            self.meta.put(&TableDefine::new(name))?;
            Ok(table)
        }
    }

    pub fn open_kv_table(&self, name: &str, config: TableConfig) -> Result<Arc<TableImpl<'a, T>>> {
        if config.temporary() && config.readonly() {
            Err(ErrorKind::Configuration.into())
        } else if !self.meta.exists(name)? {
            Err(ErrorKind::NotFound.into())
        } else {
            let table = self.open(name, config, StringKeyComparator {})?;
            Ok(table)
        }
    }

    // pub fn get(&self, name: &str) -> Option<Arc<TableImpl<'a, T>>> {
    //     self.touch();

    //     self.tables.get(name)
    // }

    pub fn close(&self, name: &str, config: &TableConfig) -> Option<Arc<TableImpl<'a, T>>> {
        self.touch();

        if config.temporary() {
            let _ = self.meta.delete(name);            
        }
        self.tables.remove(name)
    }

    // pub fn exists(&self, name: &str) -> Result<bool> {
    //     let key = name.to_entry()?;
    //     let data = self.meta.get(&key)?;
    //     Ok(data.is_some())
    // }
}

unsafe impl <'a, T: StorageEngine<'a>> Send for SchemaImpl<'a, T> {}
unsafe impl <'a, T: StorageEngine<'a>> Sync for SchemaImpl<'a, T> {}

impl <'a, T: StorageEngine<'a>> Maintainable for SchemaImpl<'a, T> {
    fn maintain(&self) {
        self.tables.cleanup(&self.config);
        let tables = self.tables.collect();
        for table in tables {
            table.maintain();
        }
    }
}

impl <'a, T: StorageEngine<'a>> Poolable for SchemaImpl<'a, T> {
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

impl <'a, T: StorageEngine<'a>> Drop for SchemaImpl<'a, T> {
    fn drop(&mut self) {
        self.meta.close();
    }
}

pub struct Schema {
    instance: Arc<InstImpl<'static, DBEngine>>,
    schema: Arc<SchemaImpl<'static, DBEngine>>,
}

impl Schema {
    pub(crate) fn new(instance: Arc<InstImpl<'static, DBEngine>>, schema: Arc<SchemaImpl<'static, DBEngine>>) -> Result<Self> {
        Ok(Self { 
            instance, 
            schema,
        })
    }
    pub fn name(&self) -> &str {
        &self.schema.name
    }

    pub fn config(&self) -> &SchemaConfig {
        self.schema.config()
    }

    pub fn create_kv_table(&self, name: &str, config: TableConfig) -> Result<KVTable> {
        let table = self.schema.create_kv_table(name, config)?;
        Ok(KVTable::new(self.schema.clone(), table))
    }

    pub fn open_kv_table(&self, name: &str, config: TableConfig) -> Result<KVTable> {
        let table = self.schema.open_kv_table(name, config)?;
        Ok(KVTable::new(self.schema.clone(), table))
    }
}

unsafe impl Send for Schema {}
unsafe impl Sync for Schema {}

impl Drop for Schema {
    fn drop(&mut self) {
        self.instance.close(self.schema.name());
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.schema.name())
    }
}
