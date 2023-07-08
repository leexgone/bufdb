use std::sync::Arc;

use bufdb_lib::db_error_s;
use bufdb_lib::error::Result;
use bufdb_storage::PrimaryCursor;
use bufdb_storage::SecondaryCursor;
use bufdb_storage::entry::BufferEntry;
use bufdb_storage::entry::Entry;
use leveldb::iterator::Iterator;
use leveldb::iterator::LevelDBIterator;

use crate::database::DBImpl;
use crate::suffix::append_suffix;
use crate::suffix::size_of_suffix;
use crate::suffix::trucate_suffix;

macro_rules! vec_to_buf {
    ($data: expr, $buf: ident) => {
        if let Some(buf) = $buf {
            buf.set_data($data);
        }
    };
}

macro_rules! buf_to_buf {
    ($src: expr, $dst: ident) => {
        if let Some(dest) = $dst {
            dest.set_buffer($src)
        }
    };
}

pub struct PKCursor<'a> {
    iter: Iterator<'a, BufferEntry>
}

impl <'a> PKCursor<'a> {
    pub(crate) fn new(db: &'a Arc<DBImpl>) -> Self {
        let iter = db.iter(read_options!());
        Self {
            iter
        }
    }
}

impl <'a> PrimaryCursor<'a> for PKCursor<'a> {
    fn search(&mut self, key: &bufdb_storage::entry::BufferEntry, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        self.iter.seek(key);

        if let Some((n_key, n_data)) = self.iter.next() {
            if *key == n_key {
                vec_to_buf!(n_data, data);

                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    fn search_range(&mut self, key: &mut bufdb_storage::entry::BufferEntry, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        self.iter.seek(key);

        self.next(Some(key), data)
    }

    fn next(&mut self, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        if let Some((n_key, n_data)) = self.iter.next() {
            buf_to_buf!(n_key, key);
            vec_to_buf!(n_data, data);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn next_dup(&mut self, _key: Option<&mut bufdb_storage::entry::BufferEntry>, _data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        Ok(false)
    }

    fn skip(&mut self, count: usize, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        let mut count = count;
        while let Some((n_key, n_data)) = self.iter.next() {
            count -= 1;
            if count == 0 {
                buf_to_buf!(n_key, key);
                vec_to_buf!(n_data, data);

                return Ok(true);
            }
        }

        Ok(false)
    }
}

pub struct IDXCursor<'a> {
    db: Arc<DBImpl>,
    iter: Iterator<'a, BufferEntry>,
    do_seek: fn (&mut Self, &BufferEntry) -> Result<()>,
    do_match: fn (&BufferEntry, &BufferEntry) -> Result<bool>,
    do_rekey: fn (&mut BufferEntry),
    do_next_dup: fn (&mut Self) -> Result<Option<(BufferEntry, Vec<u8>)>>,
}

impl <'a> IDXCursor<'a> {
    pub(crate) fn new(pdb: &'a Arc<DBImpl>, idb: &'a Arc<DBImpl>) -> Self {
        let iter = idb.iter(read_options!());
        Self { 
            db: pdb.clone(), 
            iter, 
            do_seek: if idb.unique() { Self::seek_unique } else { Self::seek_non_unique },
            do_match: if idb.unique() { Self::match_unique } else { Self::match_non_unique },
            do_rekey: if idb.unique() { Self::rekey_unique } else { Self::rekey_non_unique },
            do_next_dup: if idb.unique() { Self::next_dup_unique } else { Self::next_dup_non_unique },
        }
    }

    fn seek(&mut self, key: &BufferEntry) -> Result<()> {
        let seek_fn = &self.do_seek;
        seek_fn(self, key)
    }

    fn seek_unique(&mut self, key: &BufferEntry) -> Result<()> {
        self.iter.seek(key);
        Ok(())
    }

    fn seek_non_unique(&mut self, key: &BufferEntry) -> Result<()> {
        let skey = append_suffix(key.clone(), 0)?;
        self.iter.seek(&skey);
        Ok(())
    }

    fn match_key(&self, key: &BufferEntry, skey: &BufferEntry) -> Result<bool> {
        let match_fn = &self.do_match;
        match_fn(key, skey)
    }

    fn match_unique(key: &BufferEntry, skey: &BufferEntry) -> Result<bool> {
        Ok(key == skey)
    }

    fn match_non_unique(key: &BufferEntry, skey: &BufferEntry) -> Result<bool> {
        let slice = skey.left(skey.size() - size_of_suffix(skey))?;
        Ok(key.as_slice_entry() == slice)
    }

    fn rekey(&self, skey: BufferEntry) -> BufferEntry {
        let mut skey = skey;
        let rekey_fn = &self.do_rekey;
        rekey_fn(&mut skey);
        skey
    }

    fn rekey_unique(_skey: &mut BufferEntry) {
        // Do nothing when unique
    }

    fn rekey_non_unique(skey: &mut BufferEntry) {
        let n = size_of_suffix(skey);
        skey.set_len(skey.len() - n);
    }

    fn to_next_dup(&mut self) -> Result<Option<(BufferEntry, Vec<u8>)>> {
        let next_dup_fn = &self.do_next_dup;
        next_dup_fn(self)
    }

    fn next_dup_unique(&mut self) -> Result<Option<(BufferEntry, Vec<u8>)>> {
        Ok(None)
    }

    fn next_dup_non_unique(&mut self) -> Result<Option<(BufferEntry, Vec<u8>)>> {
        let key = self.iter.key();
        if let Some((n_key, n_data)) = self.iter.next() {
            let prev = trucate_suffix(&key)?;
            let cur = trucate_suffix(&n_key)?;
            if prev == cur {
                Ok(Some((n_key, n_data)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn fetch(&self, p_data: Vec<u8>, p_key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> Result<()> {
        if let Some(key) = p_key {
            key.set_data(p_data);
            
            if let Some(data) = data {
                if let Some(found) = self.db.get(key)? {
                    data.set_buffer(found);
                } else {
                    return Err(db_error_s!(read => "Index mismatch"));
                }
            }
        } else if let Some(data) = data {
            let key = BufferEntry::from(p_data);
            if let Some(found) = self.db.get(&key)? {
                data.set_buffer(found);
            } else {
                return Err(db_error_s!(read => "Index mismatch"));
            }
        }

        Ok(())
    }
}

impl <'a> PrimaryCursor<'a> for IDXCursor<'a> {
    fn search(&mut self, key: &bufdb_storage::entry::BufferEntry, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        self.s_search(key, data, None)
    }

    fn search_range(&mut self, key: &mut bufdb_storage::entry::BufferEntry, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        self.s_search_range(key, data, None)
    }

    fn next(&mut self, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        self.s_next(key, data, None)
    }

    fn next_dup(&mut self, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        self.s_next_dup(key, data, None)
    }

    fn skip(&mut self, count: usize, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        self.s_skip(count, key, data, None)
    }
}

impl <'a> SecondaryCursor<'a> for IDXCursor<'a> {
    fn s_search(&mut self, key: &bufdb_storage::entry::BufferEntry, p_key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        self.seek(key)?;

        if let Some((n_key, n_data)) = self.iter.next() {
            if self.match_key(key, &n_key)? {
                self.fetch(n_data, p_key, data)?;
                
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    fn s_search_range(&mut self, key: &mut bufdb_storage::entry::BufferEntry, p_key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        self.seek(key)?;

        self.s_next(Some(key), p_key, data)
    }

    fn s_next(&mut self, key: Option<&mut bufdb_storage::entry::BufferEntry>, p_key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        if let Some((n_key, n_data)) = self.iter.next() {
            buf_to_buf!(self.rekey(n_key), key);
            self.fetch(n_data, p_key, data)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn s_next_dup(&mut self, key: Option<&mut bufdb_storage::entry::BufferEntry>, p_key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_lib::error::Result<bool> {
        if let Some((n_key, n_data)) = self.to_next_dup()? {
            buf_to_buf!(self.rekey(n_key), key);
            self.fetch(n_data, p_key, data)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn s_skip(&mut self, count: usize, key: Option<&mut BufferEntry>, p_key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> bufdb_lib::error::Result<bool> {
        let mut count = count;
        while let Some((n_key, n_data)) = self.iter.next() {
            count -= 1;
            if count == 0 {
                buf_to_buf!(self.rekey(n_key), key);
                self.fetch(n_data, p_key, data)?;
                return Ok(true);
            }
        }

        Ok(false)
    }
}