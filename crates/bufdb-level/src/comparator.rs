use bufdb_storage::KeyComparator;
use bufdb_storage::entry::BufferEntry;
use leveldb::comparator::Comparator;
use libc::c_char;

pub struct PKComparator<C: KeyComparator>(C);

impl <C: KeyComparator> Comparator for PKComparator<C> {
    type K = BufferEntry;

    fn name(&self) -> *const c_char {
        "PK-Comparator".as_ptr() as *const c_char
    }

    fn compare(&self, a: &Self::K, b: &Self::K) -> std::cmp::Ordering {
        self.0.compare(a, b).unwrap()
    }
}

impl <T: KeyComparator> From<T> for PKComparator<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}