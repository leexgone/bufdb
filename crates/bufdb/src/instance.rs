use std::marker::PhantomData;
use std::sync::Arc;
use std::thread::JoinHandle;

use bufdb_api::config::InstanceConfig;
use bufdb_level::LevelDBEngine as DBEngine;
use bufdb_storage::StorageEngine;
use bufdb_storage::cache::CachePool;

use crate::daemon::Maintainable;
use crate::schema::SchemaImpl;

// type DBEngine = LevelDBEngine;

pub struct Instance<'a> {
    inst: Arc<InstImpl<'a, DBEngine>>
}

impl <'a> Instance<'a> {
    pub fn config(&self) -> &InstanceConfig {
        &self.inst.config
    }
}

impl <'a> From<Arc<InstImpl<'a, DBEngine>>> for Instance<'a> {
    fn from(inst: Arc<InstImpl<'a, DBEngine>>) -> Self {
        Self { 
            inst 
        }
    }
}

pub(crate) struct InstImpl<'a, T: StorageEngine<'a>> {
    config: InstanceConfig,
    schemas: CachePool<SchemaImpl<'a, T>>,
    _marker: PhantomData<&'a T>
}

unsafe impl <'a, T: StorageEngine<'a>> Send for InstImpl<'a, T> {}
unsafe impl <'a, T: StorageEngine<'a>> Sync for InstImpl<'a, T> {}

impl <'a, T: StorageEngine<'a>> Maintainable for InstImpl<'a, T> {
    fn maintain(&self) {
        self.schemas.cleanup(&self.config);

        let schemas = self.schemas.collect();
        for schema in schemas {
            schema.maintain();
        }
    }
}

// struct DaemonData<'a> {
//     insts: Vec<Arc<InstImpl<'a, DBEngine>>>,
//     thead: Option<JoinHandle<()>>,
//     terminated: bool,
// }

// lazy_static::lazy_static! {
//     static ref DAEMON: std::sync::RwLock<DaemonData> = std::sync::RwLock::new(DaemonData {
//         insts: Vec::new(),
//         thread: None,
//         terminated: false,
//     });
// }
// lazy_static::lazy_static! {
//     static ref DEAMON: crate::daemon::Daemon<'a, InstImpl<DBEngine>> = crate::daemon::Daemon::new();
// }