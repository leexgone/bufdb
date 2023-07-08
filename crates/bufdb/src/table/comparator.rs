use bufdb_lib::error::Result;
use bufdb_storage::KeyComparator;
use bufdb_storage::io::Input;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct StringKeyComparator {
}

impl KeyComparator for StringKeyComparator {
    fn compare<T: bufdb_storage::entry::Entry>(&self, key1: &T, key2: &T) -> Result<std::cmp::Ordering> {
        let v1 = key1.as_input().read_string()?;
        let v2 = key2.as_input().read_string()?;

        Ok(v1.cmp(&v2))
    }
}
