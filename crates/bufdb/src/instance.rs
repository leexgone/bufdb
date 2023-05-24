use std::marker::PhantomData;
use std::sync::Arc;

use bufdb_level::LevelDBFactory;
use bufdb_storage::StorageFactory;

pub struct Instance<'a> {
    inst: Arc<InstImpl<'a, LevelDBFactory>>
}

pub(crate) struct InstImpl<'a, F: StorageFactory<'a>> {
    marker: PhantomData<&'a F>
}