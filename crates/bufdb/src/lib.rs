use bufdb_level::LevelDBEngine as DBEngine;
use daemon::Daemon;
use instance::InstImpl;

pub mod instance;
pub mod schema;
pub mod table;
pub mod cursor;
pub mod daemon;

pub struct DBFactory {
    daemon: Daemon<InstImpl<'static, DBEngine>>
}

impl DBFactory {
    pub fn new() -> Self {
        Self { daemon: Daemon::new() }
    }
}