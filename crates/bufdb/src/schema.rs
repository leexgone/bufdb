use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use bufdb_level::LevelDBEngine;
use bufdb_storage::StorageEngine;
use bufdb_storage::cache::Poolable;
use bufdb_storage::get_timestamp;
use bufdb_storage::set_timestamp;

pub struct Schema<'a> {
    schema: Arc<SchemaImpl<'a, LevelDBEngine>>,
}

pub(crate) struct SchemaImpl<'a, T: StorageEngine<'a>> {
    name: String,
    last_access: AtomicI64,

    _marker: PhantomData<&'a T>
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