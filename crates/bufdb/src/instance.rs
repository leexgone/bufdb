use std::fs::create_dir_all;
use std::fs::remove_dir_all;
use std::sync::Arc;

use bufdb_api::config::InstanceConfig;
use bufdb_api::config::SchemaConfig;
use bufdb_api::error::ErrorKind;
use bufdb_api::error::Result;
use bufdb_storage::StorageEngine;
use bufdb_storage::cache::CachePool;

use crate::daemon::Daemon;
use crate::daemon::Maintainable;
use crate::engine::DBEngine;
use crate::schema::Schema;
use crate::schema::SchemaImpl;

pub(crate) struct InstImpl<'a, T: StorageEngine<'a>> {
    config: InstanceConfig,
    schemas: CachePool<SchemaImpl<'a, T>>,
}

impl <'a, T: StorageEngine<'a>> InstImpl<'a, T> {
    pub fn new(config: InstanceConfig) -> Result<Self> {
        if !config.dir().is_dir() {
            create_dir_all(config.dir())?;
        }

        Ok(Self { 
            config: config, 
            schemas: CachePool::new(), 
        })
    }

    pub fn open(&self, name: &str, config: SchemaConfig) -> Result<Arc<SchemaImpl<'a, T>>> {
        if let Some(schema) = self.schemas.get(name) {
            if config.readonly() != schema.config().readonly() || config.temporary() != schema.config().temporary() {
                Err(ErrorKind::Configuration.into())
            } else {
                Ok(schema)
            }
        } else {
            let schema = SchemaImpl::new(self.config.dir(), name, config)?;
            let schema = Arc::new(schema);
            self.schemas.put(schema.clone());
            Ok(schema)
        }
    }

    pub fn get(&self, name: &str) -> Option<Arc<SchemaImpl<'a, T>>> {
        self.schemas.get(name)
    }

    pub fn close(&self, name: &str) -> Option<Arc<SchemaImpl<'a, T>>> {
        self.schemas.remove(name)
    }
}

unsafe impl <'a, T: StorageEngine<'a>> Send for InstImpl<'a, T> {}
unsafe impl <'a, T: StorageEngine<'a>> Sync for InstImpl<'a, T> {}

impl <'a, T: StorageEngine<'a>> Maintainable for InstImpl<'a, T> {
    fn maintain(&self) {
        self.schemas.cleanup(&self.config);

        let schemas = self.schemas.collect();
        for schema in schemas {
            schema.maintain();
        }
    }
}

impl <'a, T: StorageEngine<'a>> PartialEq for InstImpl<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.config.dir() == other.config.dir()
    }
}

#[derive(Clone)]
pub struct Instance {
    daemon: Arc<Daemon<InstImpl<'static, DBEngine>>>,
    inst: Arc<InstImpl<'static, DBEngine>>,
}

unsafe impl Send for Instance {}
unsafe impl Sync for Instance {}

impl Instance {
    pub(crate) fn new(daemon: Arc<Daemon<InstImpl<'static, DBEngine>>>, config: InstanceConfig) -> Result<Self> {
        let inst = InstImpl::new(config)?;
        let inst = Arc::new(inst);

        daemon.add(inst.clone());

        Ok(Self {
            daemon, 
            inst
        })
    }

    pub fn config(&self) -> &InstanceConfig {
        &self.inst.config
    }

    pub fn open_schema(&self, name: &str, config: SchemaConfig) -> Result<Schema> {
        if config.temporary() && config.readonly() {
            Err(ErrorKind::Configuration.into())
        } else {
            let schema = self.inst.open(name, config)?;
            Ok(Schema::new(self.inst.clone(), schema))
        }
    }

    pub fn open_exist_schema(&self, name: &str) -> Option<Schema> {
        self.inst.get(name).map(|s| Schema::new(self.inst.clone(), s))
    }

    pub fn drop_schema(&self, name: &str) -> Result<bool> {
        if let Some(schema) = self.inst.close(name) {
            if let Err(schema) = Arc::try_unwrap(schema) {
                self.inst.schemas.put(schema);
                return Err(ErrorKind::CloseUsing.into());
            }
        }

        let dir = self.inst.config.dir().join(name);
        if dir.is_dir() {
            remove_dir_all(dir)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        self.daemon.remove(&self.inst);
    }
}
