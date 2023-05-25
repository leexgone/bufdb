use bufdb_storage::StorageEngine;

pub(crate) struct TableImpl<'a, F: StorageEngine<'a>> {
    db: F::DATABASE,
}