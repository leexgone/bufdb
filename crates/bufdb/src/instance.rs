use std::marker::PhantomData;
use std::sync::Arc;

use bufdb_api::config::InstanceConfig;
use bufdb_api::error::Result;
use bufdb_level::LevelDBEngine as DBEngine;
use bufdb_storage::StorageEngine;
use bufdb_storage::cache::CachePool;

use crate::daemon::Daemon;
use crate::daemon::Maintainable;
use crate::schema::SchemaImpl;

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
}

impl Drop for Instance {
    fn drop(&mut self) {
        self.daemon.remove(&self.inst);
    }
}

pub(crate) struct InstImpl<'a, T: StorageEngine<'a>> {
    config: InstanceConfig,
    schemas: CachePool<SchemaImpl<'a, T>>,
    _marker: PhantomData<&'a T>
}

impl <'a, T: StorageEngine<'a>> InstImpl<'a, T> {
    pub fn new(config: InstanceConfig) -> Result<Self> {
        Ok(Self { 
            config: config, 
            schemas: CachePool::new(), 
            _marker: PhantomData 
        })
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