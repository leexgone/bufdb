use std::fmt::Debug;

use bufdb_storage::StorageEngine;

#[derive(Clone, Copy, Default)]
pub struct DBEngine {
}

impl Debug for DBEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DBEngine")
            .field("name", &Self::name())
        .finish()
    }
}

#[cfg(feature = "leveldb")]
impl <'a> StorageEngine<'a> for DBEngine {
    // type CURSOR = bufdb_level::cursor::PKCursor<'a>;
    // type SCUROSR = bufdb_level::cursor::IDXCursor<'a>;

    // type DATABASE = bufdb_level::database::PrimaryDatabase<'a>;
    // type SDATABASE = bufdb_level::database::SecondaryDatabase<'a>;

    type ENVIRONMENT = bufdb_level::env::LevelDBEnv;

    fn name() -> &'a str {
        "LevelDB"
    }
}