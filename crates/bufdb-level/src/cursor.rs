use std::sync::Arc;

use bufdb_storage::Cursor;
use bufdb_storage::SecondaryCursor;
use bufdb_storage::entry::BufferEntry;
use leveldb::iterator::Iterator;
use leveldb::iterator::LevelDBIterator;

use crate::database::DBImpl;
use crate::read_options;

macro_rules! vec_to_buf {
    ($data: ident, $buf: ident) => {
        if let Some(buf) = $buf {
            buf.set_data($data);
        }
    };
}

macro_rules! buf_to_buf {
    ($src: ident, $dst: ident) => {
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

impl <'a> Cursor<'a> for PKCursor<'a> {
    fn search(&mut self, key: &bufdb_storage::entry::BufferEntry, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
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

    fn search_range(&mut self, key: &mut bufdb_storage::entry::BufferEntry, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        self.iter.seek(key);

        self.next(Some(key), data)
    }

    fn next(&mut self, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        if let Some((n_key, n_data)) = self.iter.next() {
            buf_to_buf!(n_key, key);
            vec_to_buf!(n_data, data);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn next_dup(&mut self, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        let ckey = self.iter.key();
        if let Some((n_key, n_data)) = self.iter.next() {
            if ckey == n_key {
                buf_to_buf!(n_key, key);
                vec_to_buf!(n_data, data);

                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    fn skip(&mut self, count: usize, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
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
    iter: Iterator<'a, BufferEntry>
}

impl <'a> IDXCursor<'a> {
    pub(crate) fn new(pdb: &'a Arc<DBImpl>, idb: &'a Arc<DBImpl>) -> Self {
        let iter = idb.iter(read_options!());
        Self { 
            db: pdb.clone(), 
            iter, 
        }
    }
}

impl <'a> Cursor<'a> for IDXCursor<'a> {
    fn search(&mut self, key: &bufdb_storage::entry::BufferEntry, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        self.s_search(key, data, None)
    }

    fn search_range(&mut self, key: &mut bufdb_storage::entry::BufferEntry, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        self.s_search_range(key, data, None)
    }

    fn next(&mut self, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        self.s_next(key, data, None)
    }

    fn next_dup(&mut self, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        self.s_next_dup(key, data, None)
    }

    fn skip(&mut self, count: usize, key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        self.s_skip(count, key, data, None)
    }
}

impl <'a> SecondaryCursor<'a> for IDXCursor<'a> {
    fn s_search(&mut self, key: &bufdb_storage::entry::BufferEntry, p_key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn s_search_range(&mut self, key: &mut bufdb_storage::entry::BufferEntry, p_key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn s_next(&mut self, key: Option<&mut bufdb_storage::entry::BufferEntry>, p_key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn s_next_dup(&mut self, key: Option<&mut bufdb_storage::entry::BufferEntry>, p_key: Option<&mut bufdb_storage::entry::BufferEntry>, data: Option<&mut bufdb_storage::entry::BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn s_skip(&mut self, count: usize, key: Option<&mut BufferEntry>, p_key: Option<&mut BufferEntry>, data: Option<&mut BufferEntry>) -> bufdb_api::error::Result<bool> {
        todo!()
    }
}