use bufdb_storage::KeyComparator;
use bufdb_storage::entry::BufferEntry;
use leveldb::comparator::Comparator;
use libc::c_char;

use crate::suffix::unwrap_suffix;

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

impl <T: KeyComparator> AsRef<T> for PKComparator<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

pub struct IDXComparator<C: KeyComparator>(C);

impl <C: KeyComparator> Comparator for IDXComparator<C> {
    type K = BufferEntry;

    fn name(&self) -> *const c_char {
        "IDX-Comparator".as_ptr() as *const c_char
    }

    fn compare(&self, a: &Self::K, b: &Self::K) -> std::cmp::Ordering {
        let (key1, ord1) = unwrap_suffix(a).unwrap();
        let (key2, ord2) = unwrap_suffix(b).unwrap();

        let c = self.0.compare(&key1, &key2).unwrap();
        if c.is_eq() {
            ord1.cmp(&ord2).reverse()
        } else {
            c
        }
    }
}

impl <T: KeyComparator> From<T> for IDXComparator<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl <T: KeyComparator> AsRef<T> for IDXComparator<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}