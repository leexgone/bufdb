use std::marker::PhantomData;
use std::sync::Arc;

use bufdb_api::config::InstanceConfig;
use bufdb_level::LevelDBEngine;
use bufdb_storage::StorageEngine;
use bufdb_storage::cache::CachePool;

use crate::schema::SchemaImpl;

pub struct Instance<'a> {
    inst: Arc<InstImpl<'a, LevelDBEngine>>
}

impl <'a> Instance<'a> {
    pub fn config(&self) -> &InstanceConfig {
        &self.inst.config
    }
}

pub(crate) struct InstImpl<'a, T: StorageEngine<'a>> {
    config: InstanceConfig,
    schemas: CachePool<'a, SchemaImpl<'a, T>>,
    _marker: PhantomData<&'a T>
}

impl <'a, T: StorageEngine<'a>> InstImpl<'a, T> {
}