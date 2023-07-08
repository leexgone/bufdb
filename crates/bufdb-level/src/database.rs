use std::fmt::Debug;
use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use bufdb_lib::db_error;
use bufdb_lib::error::Result;
use bufdb_storage::KeyComparator;
use bufdb_storage::KeyCreator;
use bufdb_storage::SDatabaseConfig;
use bufdb_storage::entry::BufferEntry;
use bufdb_storage::entry::Entry;
use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::iterator::Iterator;
use leveldb::iterator::KeyIterator;
use leveldb::iterator::LevelDBIterator;
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
use crate::suffix::reset_suffix;
use crate::suffix::unwrap_suffix;

// #[macro_export]
macro_rules! read_options {
    () => {
        leveldb::options::ReadOptions::new()
    };
    (quick) => {
        leveldb::options::ReadOptions { verify_checksums: false, fill_cache: false, snapshot: None }
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

    pub fn is_empty(&self) -> Result<bool> {
        let next = self.db.iter(read_options!(quick)).next();
        Ok(next.is_none())
    }

    pub fn unique(&self) -> bool {
        self.unique
    }

    pub fn count(&self) -> Result<usize> {
        let count = self.db.iter(read_options!(quick)).count();
        Ok(count)
    }

    pub fn put(&self, key: &BufferEntry, data: &BufferEntry) -> Result<()> {
        self.db.put(WriteOptions::new(), key, data.slice())
            .map_err(|e| db_error!(write => e))
    }

    pub fn get(&self, key: &BufferEntry) -> Result<Option<BufferEntry>> {
        self.db.get(ReadOptions::new(), key)
            .map(|data| data.map(|d| d.into()))
            .map_err(|e| db_error!(read => e))
    }

    pub fn delete(&self, key: &BufferEntry) -> Result<()> {
        self.db.delete(WriteOptions::new(), key)
            .map_err(|e| db_error!(write => e))
    }

    pub fn iter<'a>(&'a self, options: ReadOptions<'a, BufferEntry>) -> Iterator<'a, BufferEntry> {
        self.db.iter(options)
    }

    fn key_iter<'a>(&'a self, options: ReadOptions<'a, BufferEntry>) -> KeyIterator<'a, BufferEntry> {
        self.db.keys_iter(options)
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

struct IndexListener<'a> {
    idb: Arc<DBImpl>,
    creator: Arc<dyn KeyCreator + 'a>,
    on_put: fn (&Self, &BufferEntry, &BufferEntry) -> Result<()>,
    on_delete: fn (&Self, &BufferEntry, &BufferEntry) -> Result<()>,
}

impl <'a> IndexListener<'a> {
    pub fn new<G: KeyCreator + 'a>(database: Arc<DBImpl>, creator: G) -> Self {
        let unique = database.unique;
        let creator =  Arc::new(creator);

        Self { 
            idb: database, 
            creator, 
            on_put: if unique { Self::put_pk } else { Self::put_idx },
            on_delete: if unique { Self::delete_pk } else { Self::delete_idx },
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
        for (key, data) in pdb.iter(read_options!(quick)) {
            let data = BufferEntry::from(data);
            if let Some(skey) = self.creator.create_key(&key, &data)? {
                self.idb.put(&skey, &key)?;
            }
        }

        Ok(())
    }

    fn init_idx(&self, pdb: &Arc<DBImpl>) -> Result<()> {
        let mut id = 0u32;

        for (key, data) in pdb.iter(read_options!(quick)) {
            let data = BufferEntry::from(data);
            if let Some(skey) = self.creator.create_key(&key, &data)? {
                id += 1;
                let skey = append_suffix(skey, id)?;
                self.idb.put(&skey, &key)?;
            }
        };

        Ok(())
    }

    pub fn put(&self, key: &BufferEntry, data: &BufferEntry) -> Result<()> {
        let put_fn = &self.on_put;
        put_fn(self, key, data)
    }

    fn put_pk(&self, key: &BufferEntry, data: &BufferEntry) -> Result<()> {
        if let Some(ref skey) = self.creator.create_key(key, data)? {
            self.idb.put(skey, key)
        } else {
            Ok(())
        }
    }

    fn put_idx(&self, key: &BufferEntry, data: &BufferEntry) -> Result<()> {
        if let Some(skey) = self.creator.create_key(key, data)? {
            let len = skey.size();
            let skey = append_suffix(skey, 0)?;
            let s_slice = skey.left(len)?;

            let order = {
                let mut iter = self.idb.key_iter(read_options!()).from(&skey);
                if let Some(n_skey) = iter.next() {
                    let (n_slice, n) = unwrap_suffix(&n_skey)?;
                    if s_slice == n_slice {
                        n + 1
                    } else {
                        1u32
                    }
                } else {
                    1u32
                }
            };

            let skey = reset_suffix(skey, order)?;
            self.idb.put(&skey, key)
        } else {
            Ok(())
        }
    }

    pub fn delete(&self, key: &BufferEntry, data: &BufferEntry) -> Result<()> {
        let del_fn = &self.on_delete;
        del_fn(self, key, data)
    }

    fn delete_pk(&self, key: &BufferEntry, data: &BufferEntry) -> Result<()> {
        if let Some(skey) = self.creator.create_key(key, data)? {
            self.idb.delete(&skey)
        } else {
            Ok(())
        }
    }

    fn delete_idx(&self, key: &BufferEntry, data: &BufferEntry) -> Result<()> {
        if let Some(skey) = self.creator.create_key(key, data)? {
            let len = skey.size();
            let skey = append_suffix(skey, 0)?;
            let slice = skey.left(len)?;

            let mut found: Option<BufferEntry> = None;
            let mut order = u32::MAX;
            for (n_key, n_data) in self.idb.iter(read_options!()).from(&skey) {
                let (n_slice, n) = unwrap_suffix(&n_key)?;
                if n >= order || slice != n_slice {
                    break;
                }

                let n_data = BufferEntry::from(n_data);
                if *key == n_data {
                    found = Some(n_key);
                    break;
                }

                order = n;
            }

            if let Some(ref s_key) = found {
                self.idb.delete(s_key)
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }
}

impl <'a> Debug for IndexListener<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexListener").field("idb", &self.idb).field("creator", &self.creator).finish()
    }
}

#[derive(Debug)]
pub struct PrimaryDatabase<'a> {
    database: Arc<DBImpl>,
    listeners: Arc<RwLock<Vec<IndexListener<'a>>>>,
}

macro_rules! lock_db {
    ($db: ident) => {
        $db.listeners.read().unwrap()
    };
    ($db: ident => write) => {
        $db.listeners.write().unwrap()
    }
}

impl <'a> PrimaryDatabase<'a> {
    pub fn new<C: KeyComparator>(name: &str, dir: PathBuf, readonly: bool, temporary: bool, comparator: C) -> Result<Self> {
        let database = DBImpl::new(name, dir, readonly, temporary, true, comparator)?;

        Ok(Self { 
            database: Arc::new(database),
            listeners: Arc::new(RwLock::new(Vec::new())),
        })
    }

    fn register_listener<G: KeyCreator + 'a>(&self, idb: Arc<DBImpl>, creator: G) -> Result<()> {
        let mut listeners = lock_db!(self => write);

        let listener = IndexListener::new(idb, creator);
        listener.init(&self.database)?;

        listeners.push(listener);

        Ok(())
    }
}

impl <'a> bufdb_storage::Database<'a, PKCursor<'a>> for PrimaryDatabase<'a> {
    fn count(&self) -> bufdb_lib::error::Result<usize> {
        self.database.count()
    }

    fn put(&self, key: &bufdb_storage::entry::BufferEntry, data: &bufdb_storage::entry::BufferEntry) -> bufdb_lib::error::Result<()> {
        let listeners = lock_db!(self);

        if !listeners.is_empty() {
            if let Some(raw_data) = self.database.get(key)? {
                if data != &raw_data {
                    for listener in listeners.iter() {
                        listener.delete(key, &raw_data)?;
                    }
                }
            }
        }

        self.database.put(key, data)?;

        if !listeners.is_empty() {
            for listener in listeners.iter() {
                listener.put(key, data)?;
            }
        }

        Ok(())
    }

    fn get(&self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_lib::error::Result<Option<bufdb_storage::entry::BufferEntry>> {
        self.database.get(key)
    }

    fn delete(&self, key: &BufferEntry) -> Result<()> {
        let listeners = lock_db!(self);

        if listeners.is_empty() {
            self.database.delete(key)
        } else if let Some(data) = self.database.get(key)? {
            for listener in listeners.iter() {
                listener.delete(key, &data)?;
            }

            self.database.delete(key)
        } else {
            Ok(())
        }
    }

    fn delete_exist(&self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_lib::error::Result<bool> {
        if let Some(data) = self.database.get(key)? {
            let listeners = lock_db!(self);

            for listener in listeners.iter() {
                listener.delete(key, &data)?;
            }

            self.database.delete(key)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn open_cursor(&'a self) -> bufdb_lib::error::Result<PKCursor<'a>> {
        Ok(PKCursor::new(&self.database))
    }
}

#[derive(Debug)]
pub struct SecondaryDatabase<'a> {
    database: Arc<DBImpl>,
    parent: Arc<DBImpl>,
    listeners: Arc<RwLock<Vec<IndexListener<'a>>>>
}

impl <'a> SecondaryDatabase<'a> {
    pub fn new<C: KeyComparator, G: KeyCreator + 'a>(p_database: &PrimaryDatabase<'a>, name: &str, config: SDatabaseConfig<C, G>) -> Result<Self> {
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

impl <'a> Drop for SecondaryDatabase<'a> {
    fn drop(&mut self) {
        let mut listeners = self.listeners.write().unwrap();
        listeners.retain(|x| x.idb != self.database);
    }
}

impl <'a> bufdb_storage::Database<'a, IDXCursor<'a>> for SecondaryDatabase<'a> {
    fn count(&self) -> bufdb_lib::error::Result<usize> {
        self.database.count()
    }

    fn put(&self, key: &bufdb_storage::entry::BufferEntry, data: &bufdb_storage::entry::BufferEntry) -> bufdb_lib::error::Result<()> {
        self.database.put(key, data)
    }

    fn get(&self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_lib::error::Result<Option<bufdb_storage::entry::BufferEntry>> {
        self.database.get(key)
    }

    fn delete(&self, key: &BufferEntry) -> Result<()> {
        self.database.delete(key)
    }

    fn delete_exist(&self, key: &bufdb_storage::entry::BufferEntry) -> bufdb_lib::error::Result<bool> {
        let data = self.database.get(key)?;
        if data.is_some() {
            self.database.delete(key)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn open_cursor(&self) -> bufdb_lib::error::Result<IDXCursor> {
        Ok(IDXCursor::new(&self.parent, &self.database))
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