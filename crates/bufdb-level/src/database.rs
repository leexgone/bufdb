use std::fmt::Debug;
use std::fmt::Display;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use bufdb_api::db_error;
use bufdb_api::error::Result;
use bufdb_storage::KeyComparator;
use bufdb_storage::entry::BufferEntry;
use leveldb::comparator::Comparator;
use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::kv::KV;
use leveldb::options::Options;
use leveldb::options::ReadOptions;
use leveldb::options::WriteOptions;
use leveldb_sys::Compression;

use crate::comparator::IDXComparator;
use crate::comparator::PKComparator;
use crate::cursor::IDXCursor;
use crate::cursor::PKCursor;

pub(crate) struct DBImpl{
    name: String,
    dir: PathBuf,
    readonly: bool,
    temporary: bool,
    unique: bool,
    db: Database<BufferEntry>
}

impl DBImpl {
    fn new<C: bufdb_storage::KeyComparator>(name: &str, dir: PathBuf, readonly: bool, temporary: bool, unique: bool, comparator: C) -> Result<DBImpl> {
        let mut options = Options::new();
        options.create_if_missing = !readonly;
        options.compression = Compression::Snappy;

        let raw_db = if unique {
            Database::open_with_comparator(&dir, options, PKComparator::from(comparator))
        } else {
            Database::open_with_comparator(&dir, options, IDXComparator::from(comparator))
        };

        let db = match raw_db {
            Ok(db) => db,
            Err(e) => return Err(db_error!(open => e)),
        };

        Ok(DBImpl {
            name: name.into(),
            dir,
            readonly,
            temporary,
            unique,
            db
        })
    }

    fn count(&self) -> Result<usize> {
        let count = self.db.iter(ReadOptions::new()).count();
        Ok(count)
    }

    fn put(&self, key: &BufferEntry, data: &BufferEntry) -> Result<()> {
        self.db.put(WriteOptions::new(), key, data.slice())
            .map_err(|e| db_error!(write => e))
    }

    fn get(&self, key: &BufferEntry) -> Result<Option<BufferEntry>> {
        self.db.get(ReadOptions::new(), key)
            .map(|data| data.map(|d| d.into()))
            .map_err(|e| db_error!(read => e))
    }

    fn delete(&self, key: &BufferEntry) -> Result<()> {
        self.db.delete(WriteOptions::new(), key)
            .map_err(|e| db_error!(write => e))
    }
}

impl Display for DBImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl PartialEq for DBImpl {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for DBImpl {}

impl Debug for DBImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DBImpl")
            .field("name", &self.name)
            .field("dir", &self.dir.to_string_lossy())
            .field("readonly", &self.readonly)
            .field("temporary", &self.temporary)
            .field("unique", &self.unique)
            .finish()
    }
}

#[derive(Debug)]
struct IndexListener {
    idb: Arc<DBImpl>
}

#[derive(Debug)]
pub struct PrimaryDatabase {
    database: Arc<DBImpl>,
    listeners: Arc<RwLock<Vec<IndexListener>>>,
}

impl PrimaryDatabase {
    pub fn new<C: bufdb_storage::KeyComparator>(name: &str, dir: PathBuf, readonly: bool, temporary: bool, comparator: C) -> Result<Self> {
        let database = DBImpl::new(name, dir, readonly, temporary, true, comparator)?;

        Ok(Self { 
            database: Arc::new(database),
            listeners: Arc::new(RwLock::new(Vec::new())),
        })
    }

    // pub fn dir(&self) -> &Path {
    //     &self.database.dir
    // }

    // pub fn readonly(&self) -> bool {
    //     self.database.readonly
    // }

    // pub fn temporary(&self) -> bool {
    //     self.database.temporary
    // }
}

impl bufdb_storage::Database<PKCursor> for PrimaryDatabase {
    fn count(&self) -> bufdb_api::error::Result<usize> {
        self.database.count()
    }

    fn put(&self, key: &bufdb_storage::entry::BufferEntry, data: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<()> {
        self.database.put(key, data)
    }

    fn get(&self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<Option<bufdb_storage::entry::BufferEntry>> {
        self.database.get(key)
    }

    fn delete(&self, key: &BufferEntry) -> Result<()> {
        self.database.delete(key)
    }

    fn delete_exist(&self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
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
    database: Arc<DBImpl>,
    parent: Arc<DBImpl>,
    listeners: Arc<RwLock<Vec<IndexListener>>>
}

impl SecondaryDatabase {
    pub fn new<C: KeyComparator>(p_database: &PrimaryDatabase, name: &str, temporary: bool, unique: bool, comparator: C) -> Result<SecondaryDatabase> {
        let parent = p_database.database.clone();

        let mut dir = parent.dir.clone();
        dir.push(name);

        let db = DBImpl::new(name, dir, parent.readonly, temporary || parent.temporary, unique, comparator)?;

        Ok(Self { 
            database: Arc::new(db), 
            parent, 
            listeners: p_database.listeners.clone() 
        })
    }
}

impl bufdb_storage::Database<IDXCursor> for SecondaryDatabase {
    fn count(&self) -> bufdb_api::error::Result<usize> {
        todo!()
    }

    fn put(&self, key: &bufdb_storage::entry::BufferEntry, data: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<()> {
        todo!()
    }

    fn get(&self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<Option<bufdb_storage::entry::BufferEntry>> {
        todo!()
    }

    fn delete(&self, key: &BufferEntry) -> Result<()> {
        todo!()
    }

    fn delete_exist(&self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_api::error::Result<bool> {
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

    #[derive(Debug)]
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
            assert_eq!(counter.msg, c.msg);

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