use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use bufdb_api::config::TableConfig;
use bufdb_api::error::Result;
use bufdb_storage::Database;
use bufdb_storage::DatabaseConfig;
use bufdb_storage::Environment;
use bufdb_storage::KeyComparator;
use bufdb_storage::StorageEngine;
use bufdb_storage::cache::Poolable;
use bufdb_storage::cache::now;
use bufdb_storage::get_timestamp;
use bufdb_storage::io::Input;
use bufdb_storage::io::Inputable;
use bufdb_storage::io::Outputable;
use bufdb_storage::set_timestamp;

use crate::daemon::Maintainable;
use crate::engine::DBEngine;
use crate::schema::SchemaImpl;

pub(crate) struct TableImpl<'a, T: StorageEngine<'a>> {
    name: String,
    config: TableConfig,
    db: <<T as StorageEngine<'a>>::ENVIRONMENT as Environment<'a>>::DATABASE,
    last_access: AtomicI64,
}

impl <'a, T: StorageEngine<'a>> TableImpl<'a, T> {
    pub fn new<S: Into<String>, C: KeyComparator>(env: &T::ENVIRONMENT, name: S, config: TableConfig, comparator: C) -> Result<Self> {
        let name: String = name.into();

        let db_config = DatabaseConfig {
            readonly: config.readonly(),
            temporary: config.temporary(),
            comparator
        };
        let db = env.create_database(&name, db_config)?;
        
        Ok(Self { 
            name, 
            config, 
            db, 
            last_access: AtomicI64::new(now()) 
        })
    }

    pub fn config(&self) -> &TableConfig {
        &self.config
    }
}

unsafe impl <'a, T: StorageEngine<'a>> Send for TableImpl<'a, T> {}
unsafe impl <'a, T: StorageEngine<'a>> Sync for TableImpl<'a, T> {}

impl <'a, T: StorageEngine<'a>> Maintainable for TableImpl<'a, T> {
    fn maintain(&self) {
    }
}

impl <'a, T: StorageEngine<'a>> Poolable for TableImpl<'a, T> {
    fn name(&self) -> &str {
        &self.name
    }

    fn last_access(&self) -> i64 {
        get_timestamp!(self.last_access)
    }

    fn touch(&self) {
        set_timestamp!(self.last_access)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct StringKeyComparator {
}

impl KeyComparator for StringKeyComparator {
    fn compare<T: bufdb_storage::entry::Entry>(&self, key1: &T, key2: &T) -> Result<std::cmp::Ordering> {
        let v1 = key1.as_input().read_string()?;
        let v2 = key2.as_input().read_string()?;

        Ok(v1.cmp(&v2))
        // if let Some(s1) = v1 {
        //     if let Some(s2) = v2 {
        //         Ok(s1.cmp(&s2))
        //     } else {
        //         Ok(std::cmp::Ordering::Greater)
        //     }
        // } else {
        //     if v2.is_some() {
        //         Ok(std::cmp::Ordering::Less)
        //     } else {
        //         Ok(std::cmp::Ordering::Equal)
        //     }
        // }
    }
}

pub struct KVTable {
    schema: Arc<SchemaImpl<'static, DBEngine>>,
    table: Arc<TableImpl<'static, DBEngine>>,
}

impl KVTable {
    pub(crate) fn new(schema: Arc<SchemaImpl<'static, DBEngine>>, table: Arc<TableImpl<'static, DBEngine>>) -> Self {
        Self { 
            schema, 
            table 
        }
    }

    pub fn name(&self) -> &str {
        self.table.name()
    }

    pub fn config(&self) -> &TableConfig {
        self.table.config()
    }

    pub fn put<V: Outputable>(&self, key: &str, value: V) -> Result<()> {
        let k = key.to_entry()?;
        let v = value.to_entry()?;

        self.table.db.put(&k, &v)
    }

    pub fn get<V: Inputable>(&self, key: &str) -> Result<Option<V>> {
        let k = key.to_entry()?;
        if let Some(data) = self.table.db.get(&k)? {
            let v = V::from_entry(&data)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    pub fn get_or<V: Inputable>(&self, key: &str, default: V) -> Result<V> {
        let v = self.get(key)?;
        Ok(v.unwrap_or(default))
    }

    pub fn get_or_else<V: Inputable, F: FnOnce() -> V>(&self, key: &str, f: F) -> Result<V> {
        let v = self.get(key)?;
        Ok(v.unwrap_or_else(f))
    }

    pub fn get_or_default<V: Inputable + Default>(&self, key: &str) -> Result<V> {
        let v = self.get(key)?;
        Ok(v.unwrap_or_default())
    }

    pub fn exists(&self, key: &str) -> Result<bool> {
        let k = key.to_entry()?;
        let data = self.table.db.get(&k)?;
        Ok(data.is_some())
    }
}

impl Drop for KVTable {
    fn drop(&mut self) {
        self.schema.close(self.table.name(), self.config());
    }
}

impl Display for KVTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.table.name())
    }
}