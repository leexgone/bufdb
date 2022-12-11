use crate::error::ErrorKind;
use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Default)]
enum Value {
    #[default]
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
    values: Vec<Value>
}

impl Record {
    pub fn new(len: usize) -> Record {
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(Value::default());
        }

        Record { values }    
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_null(&self, index: usize) -> Result<bool> {
        if let Some(val) = self.values.get(index) {
            Ok(val == &Value::NULL)
        } else {
            Err(ErrorKind::OutOfBounds.into())
        }
    }

    pub fn set_null(&mut self, index: usize) -> Result<()> {
        if let Some(val) = self.values.get_mut(index) {
            *val = Value::NULL;
            Ok(())
        } else {
            Err(ErrorKind::OutOfBounds.into())
        }
    }
}