use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use bufdb_api::config::SchemaConfig;
use bufdb_storage::StorageEngine;
use bufdb_storage::cache::CachePool;
use bufdb_storage::cache::Poolable;
use bufdb_storage::get_timestamp;
use bufdb_storage::set_timestamp;

use crate::daemon::Maintainable;
use crate::engine::DBEngine;
use crate::table::TableImpl;

pub struct Schema<'a> {
    schema: Arc<SchemaImpl<'a, DBEngine>>,
}

impl <'a> Schema<'a> {
    pub fn name(&self) -> &str {
        &self.schema.name
    }
}

pub(crate) struct SchemaImpl<'a, T: StorageEngine<'a>> {
    name: String,
    config: SchemaConfig,
    tables: CachePool<TableImpl<'a, T>>,
    last_access: AtomicI64,

    _marker: PhantomData<&'a T>
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