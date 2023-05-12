use std::path::Path;
use std::path::PathBuf;

use bufdb_api::db_error;
use bufdb_api::error::Result;
use bufdb_storage::entry::BufferEntry;
use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::kv::KV;
use leveldb::options::Options;
use leveldb::options::ReadOptions;
use leveldb::options::WriteOptions;
use leveldb_sys::Compression;

use crate::comparator::PKComparator;
use crate::cursor::IDXCursor;
use crate::cursor::PKCursor;

struct DBImpl(Database<BufferEntry>);

impl DBImpl {
    fn count(&self) -> Result<usize> {
        let count = self.0.iter(ReadOptions::new()).count();
        Ok(count)
    }

    fn put(&self, key: &BufferEntry, data: &BufferEntry) -> Result<()> {
        self.0.put(WriteOptions::new(), key, data.slice())
            .map_err(|e| db_error!(write => e))
    }

    fn get(&self, key: &BufferEntry) -> Result<Option<BufferEntry>> {
        self.0.get(ReadOptions::new(), key)
            .map(|data| data.map(|d| d.into()))
            .map_err(|e| db_error!(read => e))
    }

    fn delete(&self, key: &BufferEntry) -> Result<()> {
        self.0.delete(WriteOptions::new(), key)
            .map_err(|e| db_error!(write => e))
    }
}

pub struct PrimaryDatabase {
    dir: PathBuf,
    readonly: bool,
    temporary: bool,
    database: DBImpl,
}

impl PrimaryDatabase {
    pub fn new<C: bufdb_storage::KeyComparator>(dir: PathBuf, readonly: bool, temporary: bool, comparator: C) -> Result<Self> {
        let mut options = Options::new();
        options.create_if_missing = !readonly;
        options.compression = Compression::Snappy;

        let database = match Database::open_with_comparator(&dir, options, PKComparator::from(comparator)) {
            Ok(db) => db,
            Err(e) => return Err(db_error!(open => e)),
        };

        Ok(Self { 
            dir, 
            readonly, 
            temporary, 
            database: DBImpl(database)
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn readonly(&self) -> bool {
        self.readonly
    }

    pub fn temporary(&self) -> bool {
        self.temporary
    }
}

impl bufdb_storage::Database<PKCursor> for PrimaryDatabase {
    fn count(&self) -> bufdb_api::error::Result<usize> {
        self.database.count()
    }

    fn put(&mut self, key: &bufdb_storage::entry::BufferEntry, data: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<()> {
        self.database.put(key, data)
    }

    fn get(&self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<Option<bufdb_storage::entry::BufferEntry>> {
        self.database.get(key)
    }

    fn delete(&mut self, key: &BufferEntry) -> Result<()> {
        self.database.delete(key)
    }

    fn delete_exist(&mut self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
        let data = self.database.get(key)?;
        if data.is_some() {
            self.database.delete(key)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn open_cursor(&self) -> bufdb_api::error::Result<PKCursor> {
        todo!()
    }
}

#[derive(Debug)]
pub struct SecondaryDatabase {
    // p_db: Weak
}

impl bufdb_storage::Database<IDXCursor> for SecondaryDatabase {
    fn count(&self) -> bufdb_api::error::Result<usize> {
        todo!()
    }

    fn put(&mut self, key: &bufdb_storage::entry::BufferEntry, data: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<()> {
        todo!()
    }

    fn get(&self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<Option<bufdb_storage::entry::BufferEntry>> {
        todo!()
    }

    fn delete(&mut self, key: &BufferEntry) -> Result<()> {
        todo!()
    }

    fn delete_exist(&mut self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
        todo!()
    }

    fn open_cursor(&self) -> bufdb_api::error::Result<IDXCursor> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::thread;

    struct Counter {
        msg: String,
        count: RwLock<u32>
    }

    #[test]
    fn test_arc() {
        let counter = Arc::new(Counter {
            msg: String::from("hello"),
            count: RwLock::new(0u32),
        });
        
        let mut threads = vec![];

        for _ in 0..10 {
            let c = counter.clone();
            let t = thread::spawn(move || {
                {
                    println!("find {}, {}", &c.msg, c.count.read().unwrap());
                };

                let mut v = c.count.write().unwrap();
                *v += 1;

                println!("write {}", v);
            });
            threads.push(t);
        }

        for t in threads {
            t.join().unwrap();
        }

        println!("count = {}", counter.count.read().unwrap());
    }
}