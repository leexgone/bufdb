use bufdb_api::error::ErrorKind;
use bufdb_api::error::Result;
use bufdb_api::model::TableDefine;
use bufdb_storage::Database;
use bufdb_storage::Environment;
use bufdb_storage::StorageEngine;
use bufdb_storage::io::Inputable;
use bufdb_storage::io::Outputable;

pub(super) struct MetaStorage<'a, T: StorageEngine<'a>> {
    pub db: Option<<<T as StorageEngine<'a>>::ENVIRONMENT as Environment<'a>>::DATABASE>,
}

impl <'a, T: StorageEngine<'a>> MetaStorage<'a, T> {
    pub fn exists(&self, name: &str) -> Result<bool> {
        if let Some(ref db) = self.db {
            let key = name.to_entry()?;
            let data = db.get(&key)?;
            Ok(data.is_some())
        } else {
            Err(ErrorKind::AlreadyClosed.into())
        }
    }

    pub fn put(&self, define: &TableDefine) -> Result<()> {
        if let Some(ref db) = self.db {
            let json: String = define.try_into()?;
            let key = define.name.to_entry()?;
            let data = json.to_entry()?;
            db.put(&key, &data)
        } else {
            Err(ErrorKind::AlreadyClosed.into())
        }
    }

    pub fn get(&self, name: &str) -> Result<Option<TableDefine>> {
        if let Some(ref db) = self.db {
            let key = name.to_entry()?;
            if let Some(data) = db.get(&key)? {
                let json = String::from_entry(&data)?;
                let define = TableDefine::try_from(json.as_str())?;
                Ok(Some(define))
            } else {
                Ok(None)
            }
        } else {
            Err(ErrorKind::AlreadyClosed.into())
        }
    }

    pub fn delete(&self, name: &str) -> Result<()> {
        if let Some(ref db) = self.db {
            let key = name.to_entry()?;
            db.delete(&key)
        } else {
            Err(ErrorKind::AlreadyClosed.into())
        }
    }

    pub fn close(&mut self) {
        self.db = None
    }
}