use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use bufdb_api::config::SchemaConfig;
use bufdb_api::error::Result;
use bufdb_storage::Environment;
use bufdb_storage::EnvironmentConfig;
use bufdb_storage::StorageEngine;
use bufdb_storage::cache::CachePool;
use bufdb_storage::cache::Poolable;
use bufdb_storage::cache::now;
use bufdb_storage::get_timestamp;
use bufdb_storage::set_timestamp;

use crate::daemon::Maintainable;
use crate::engine::DBEngine;
use crate::instance::InstImpl;
use crate::table::TableImpl;

pub(crate) struct SchemaImpl<'a, T: StorageEngine<'a>> {
    name: String,
    config: SchemaConfig,
    env: T::ENVIRONMENT,
    tables: CachePool<TableImpl<'a, T>>,
    last_access: AtomicI64,
}

impl <'a, T: StorageEngine<'a>> SchemaImpl<'a, T> {
    pub fn new<S: Into<String>>(inst_dir: &Path, name: S, config: SchemaConfig) -> Result<Self> {
        let name: String = name.into();

        let env_config = EnvironmentConfig {
            dir: inst_dir.join(&name),
            readonly: config.readonly(),
            temporary: config.temporary(),
        };
        let env = T::ENVIRONMENT::new(env_config)?;
        
        Ok(Self { 
            name, 
            config, 
            env, 
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

pub struct Schema {
    instance: Arc<InstImpl<'static, DBEngine>>,
    schema: Arc<SchemaImpl<'static, DBEngine>>,
}

impl Schema {
    pub(crate) fn new(instance: Arc<InstImpl<'static, DBEngine>>, schema: Arc<SchemaImpl<'static, DBEngine>>) -> Self {
        Self { 
            instance, 
            schema 
        }
    }
    pub fn name(&self) -> &str {
        &self.schema.name
    }

    pub fn config(&self) -> &SchemaConfig {
        self.schema.config()
    }
}

unsafe impl Send for Schema {}
unsafe impl Sync for Schema {}

impl Drop for Schema {
    fn drop(&mut self) {
        self.instance.close(self.schema.name());
    }
}
