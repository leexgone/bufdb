use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::iterator::LevelDBIterator;
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
    for i in [1, 3, 5, 7, 10] {
        database.put(write_opts, i, &[i as u8]).unwrap();
    }

    let read_opts = ReadOptions::new();
    let ret = database.get(read_opts, 1).unwrap();
    println!("read data : {:?}", ret);

    let mut iter = database.iter(ReadOptions::new()).from(&4);
    let ret = iter.next().unwrap();
    println!("found data: {:?}", ret);
}
