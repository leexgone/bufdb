use std::sync::atomic::AtomicI64;

use bufdb_storage::StorageEngine;
use bufdb_storage::cache::Poolable;
use bufdb_storage::get_timestamp;
use bufdb_storage::set_timestamp;

use crate::daemon::Maintainable;

pub(crate) struct TableImpl<'a, F: StorageEngine<'a>> {
    name: String,
    last_access: AtomicI64,
    _db: F::DATABASE,
}

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