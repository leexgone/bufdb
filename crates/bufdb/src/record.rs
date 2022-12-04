#[derive(Debug, Clone)]
enum DataValue {
    NULL,
    STRING(String),
    DOUBLE(f64),
    INTEGER(i32),
    LONG(i64),
    DATETIME(u64),
    BOOLEAN(bool),
    BLOB(Vec<u8>)
}

#[derive(Debug, Clone)]
pub struct Record {
    values: Vec<DataValue>
}