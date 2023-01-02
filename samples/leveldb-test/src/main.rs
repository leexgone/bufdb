use leveldb::database::Database;
use leveldb::kv::KV;
use leveldb::options::Options;
use leveldb::options::ReadOptions;
use leveldb::options::WriteOptions;
use tempdir::TempDir;

fn main() {
    let tempdir = TempDir::new("leveldb").unwrap();
    let path = tempdir.path();

    let mut options = Options::new();
    options.create_if_missing = true;

    let database = match Database::open(path, options) {
        Ok(db) => { db },
        Err(e) => { panic!("failed to open database: {:?}", e) }
    };

    let write_opts = WriteOptions::new();
    database.put(write_opts, 1, &[1]).unwrap();

    let read_opts = ReadOptions::new();
    let ret = database.get(read_opts, 1).unwrap();
    println!("read data : {:?}", ret);
}
