use std::sync::Arc;
use std::sync::OnceLock;

use bufdb_api::config::InstanceConfig;
use bufdb_api::error::Result;
use daemon::Daemon;
use engine::DBEngine;
use instance::InstImpl;
use instance::Instance;

pub mod instance;
pub mod schema;
pub mod table;
pub mod cursor;

pub(crate) mod daemon;
pub(crate) mod engine;

struct DBFactory {
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

static FACTORY: OnceLock<DBFactory> = OnceLock::new();

pub fn new_instance(config: InstanceConfig) -> Result<Instance> {
    let factory = FACTORY.get_or_init(|| DBFactory::new());

    factory.create_instance(config)
}