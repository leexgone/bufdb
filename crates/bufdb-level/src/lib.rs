use bufdb_storage::StorageEngine;
use cursor::IDXCursor;
use cursor::PKCursor;
use database::PrimaryDatabase;
use database::SecondaryDatabase;
use env::LevelDBEnv;

pub mod env;
#[macro_use]
pub mod database;
pub mod cursor;
pub(crate) mod comparator;
pub(crate) mod suffix;

#[derive(Debug, Clone, Copy)]
pub struct LevelDBEngine {}

impl <'a> StorageEngine<'a> for LevelDBEngine {
    type CURSOR = PKCursor<'a>;
    type SCUROSR = IDXCursor<'a>;

    type DATABASE = PrimaryDatabase<'a>;
    type SDATABASE = SecondaryDatabase<'a>;

    type ENVIRONMENT = LevelDBEnv;

    fn name(&self) -> &str {
        "Level DB Engine"
    }
}