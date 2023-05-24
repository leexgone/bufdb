use bufdb_storage::StorageFactory;

pub(crate) struct TableImpl<'a, F: StorageFactory<'a>> {
    db: F::DATABASE,
}