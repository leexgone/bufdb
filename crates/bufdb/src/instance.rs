use std::marker::PhantomData;
use std::sync::Arc;

use bufdb_level::LevelDBEngine;
use bufdb_storage::StorageEngine;

pub struct Instance<'a> {
    inst: Arc<InstImpl<'a, LevelDBEngine>>
}

pub(crate) struct InstImpl<'a, T: StorageEngine<'a>> {
    _marker: PhantomData<&'a T>
}