use std::sync::Arc;

use bufdb_api::config::InstanceConfig;
use bufdb_api::error::Result;
use bufdb_level::LevelDBEngine as DBEngine;
use daemon::Daemon;
use instance::InstImpl;
use instance::Instance;

pub mod instance;
pub mod schema;
pub mod table;
pub mod cursor;

pub(crate) mod daemon;

pub struct DBFactory {
    daemon: Arc<Daemon<InstImpl<'static, DBEngine>>>
}

impl DBFactory {
    pub fn new() -> Self {
        Self { 
            daemon: Arc::new(Daemon::new())
        }
    }

    pub fn create_instance(&self, config: InstanceConfig) -> Result<Instance> {
        Instance::new(self.daemon.clone(), config)
    }
}