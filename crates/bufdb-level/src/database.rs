use std::fmt::Debug;
use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use bufdb_api::db_error;
use bufdb_api::error::Result;
use bufdb_storage::KeyComparator;
use bufdb_storage::KeyCreator;
use bufdb_storage::SDatabaseConfig;
use bufdb_storage::entry::BufferEntry;
use bufdb_storage::entry::Entry;
use bufdb_storage::entry::SliceEntry;
use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::iterator::Iterator;
use leveldb::kv::KV;
use leveldb::options::Options;
use leveldb::options::ReadOptions;
use leveldb::options::WriteOptions;
use leveldb_sys::Compression;

use crate::comparator::IDXComparator;
use crate::comparator::PKComparator;
use crate::cursor::IDXCursor;
use crate::cursor::PKCursor;
use crate::suffix::append_suffix;

macro_rules! read_options {
    () => {
        ReadOptions::new()
    };
    (quick) => {
        ReadOptions { verify_checksums: false, fill_cache: false, snapshot: None }
    };
}

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

    fn is_empty(&self) -> Result<bool> {
        let next = self.db.iter(read_options!(quick)).next();
        Ok(next.is_none())
    }

    fn count(&self) -> Result<usize> {
        let count = self.db.iter(read_options!(quick)).count();
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

    fn iter<'a>(&'a self, options: ReadOptions<'a, BufferEntry>) -> Result<Iterator<'a, BufferEntry>> {
        Ok(self.db.iter(options))
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

struct IndexListener {
    idb: Arc<DBImpl>,
    creator: Box<dyn KeyCreator>,
}

impl IndexListener {
    pub fn new<G: KeyCreator>(database: Arc<DBImpl>, creator: G) -> Self {
        // let unique = database.unique;

        Self { 
            idb: database, 
            creator: Box::new(creator), 
        }
    }

    pub fn init(&self, pdb: &Arc<DBImpl>) -> Result<()> {
        if self.idb.is_empty()? {
            if self.idb.unique {
                self.init_pk(pdb)
            } else {
                self.init_idx(pdb)
            }
        } else {
            Ok(())
        }
    }

    fn init_pk(&self, pdb: &Arc<DBImpl>) -> Result<()> {
        for (key, data) in pdb.iter(read_options!(quick))? {
            let data = SliceEntry::new(&data);
            if let Some(skey) = self.creator.create_key(&key.as_slice_entry(), &data)? {
                self.idb.put(&skey, &key)?;
            }
        }

        Ok(())
    }

    fn init_idx(&self, pdb: &Arc<DBImpl>) -> Result<()> {
        let mut id = 0u32;

        for (key, data) in pdb.iter(read_options!(quick))? {
            let data = SliceEntry::new(&data);
            if let Some(mut skey) = self.creator.create_key(&key.as_slice_entry(), &data)? {
                id += 1;
                append_suffix(&mut skey, id);
                self.idb.put(&skey, &key)?;
            }
        };

        Ok(())
    }
}

impl Debug for IndexListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexListener").field("idb", &self.idb).field("creator", &self.creator).finish()
    }
}

#[derive(Debug)]
pub struct PrimaryDatabase {
    database: Arc<DBImpl>,
    listeners: Arc<RwLock<Vec<IndexListener>>>,
}

impl PrimaryDatabase {
    pub fn new<C: KeyComparator>(name: &str, dir: PathBuf, readonly: bool, temporary: bool, comparator: C) -> Result<Self> {
        let database = DBImpl::new(name, dir, readonly, temporary, true, comparator)?;

        Ok(Self { 
            database: Arc::new(database),
            listeners: Arc::new(RwLock::new(Vec::new())),
        })
    }

    fn register_listener<G: KeyCreator>(&self, idb: Arc<DBImpl>, creator: G) -> Result<()> {
        let listener = IndexListener {
            idb,
            creator: Box::new(creator),
        };

        listener.init(&self.database)?;

        {
            let mut listeners = self.listeners.write().unwrap();
            listeners.push(listener);
        }

        Ok(())
    }

    fn is_listened(&self) -> bool {
        let listeners = self.listeners.read().unwrap();
        !listeners.is_empty()
    }
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
    pub fn new<C: KeyComparator, G: KeyCreator>(p_database: &PrimaryDatabase, name: &str, config: SDatabaseConfig<C, G>) -> Result<SecondaryDatabase> {
        let parent = p_database.database.clone();

        let mut dir = parent.dir.clone();
        dir.push(name);

        let db = DBImpl::new(name, dir, parent.readonly, config.temporary || parent.temporary, config.unique, config.comparator)?;
        let database = Arc::new(db);

        p_database.register_listener(database.clone(), config.creator)?;

        Ok(Self { 
            database, 
            parent, 
            listeners: p_database.listeners.clone() 
        })
    }
}

impl Drop for SecondaryDatabase {
    fn drop(&mut self) {
        {
            let mut listeners = self.listeners.write().unwrap();
            listeners.retain(|x| x.idb != self.database);
        }
    }
}

impl bufdb_storage::Database<IDXCursor> for SecondaryDatabase {
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