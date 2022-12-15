use crate::error::ErrorKind;
use crate::error::Result;

/// Defines `TimeStamp` type to store datetime.
pub type TimeStamp = u64;

#[derive(Debug, Clone, PartialEq, Default)]
enum Value {
    #[default]
    NULL,
    STRING(Box<String>),
    DOUBLE(f64),
    INTEGER(i32),
    LONG(i64),
    DATETIME(TimeStamp),
    BOOLEAN(bool),
    BLOB(Box<Vec<u8>>)
}

impl Value {
    fn is_null(&self) -> bool {
        self == &Value::NULL
    }
}

impl From<&str> for Value {
    fn from(val: &str) -> Self {
        Self::STRING(Box::new(val.into()))
    }
}

impl From<String> for Value {
    fn from(val: String) -> Self {
        Self::STRING(Box::new(val))
    }
}

impl From<f64> for Value {
    fn from(val: f64) -> Self {
        Self::DOUBLE(val)
    }
}

impl From<i32> for Value {
    fn from(val: i32) -> Self {
        Self::INTEGER(val)
    }
}

impl From<i64> for Value {
    fn from(val: i64) -> Self {
        Self::LONG(val)
    }
}

impl From<TimeStamp> for Value {
    fn from(val: TimeStamp) -> Self {
        Self::DATETIME(val)
    }
}

impl From<bool> for Value {
    fn from(val: bool) -> Self {
        Self::BOOLEAN(val)
    }
}

impl From<&[u8]> for Value {
    fn from(val: &[u8]) -> Self {
        Self::BLOB(Box::new(val.into()))
    }
}

impl From<Vec<u8>> for Value {
    fn from(val: Vec<u8>) -> Self {
        Self::BLOB(Box::new(val))
    }
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

    fn get(&self, index: usize) -> Result<&Value> {
        if let Some(val) = self.values.get(index) {
            Ok(val)
        } else {
            Err(ErrorKind::OutOfBounds.into())
        }
    }

    fn get_mut(&mut self, index: usize) -> Result<&mut Value> {
        if let Some(val) = self.values.get_mut(index) {
            Ok(val)
        } else {
            Err(ErrorKind::OutOfBounds.into())
        }
    }

    pub fn is_null(&self, index: usize) -> Result<bool> {
        let val = self.get(index)?;
        Ok(val.is_null())
    }

    pub fn set_null(&mut self, index: usize) -> Result<()> {
        let val = self.get_mut(index)?;
        *val = Value::NULL;
        Ok(())
    }

    fn set_value<T>(&mut self, index: usize, v: T) -> Result<()> where T: Into<Value> {
        let val = self.get_mut(index)?;
        *val = v.into();
        Ok(())
    }

    pub fn get_str(&self, index: usize) -> Result<Option<&str>> {
         let val = self.get(index)?;
        match val {
            Value::STRING(v) => Ok(Some(v.as_ref())),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::ErrorType.into())
        }
    }

    pub fn set_str(&mut self, index: usize, str: &str) -> Result<()> {
        self.set_value(index, str)
    }

    pub fn set_string(&mut self, index: usize, s: String) -> Result<()> {
        self.set_value(index, s)
    }

    pub fn get_double(&self, index: usize) -> Result<Option<f64>> {
        let val = self.get(index)?;
        match val {
            Value::DOUBLE(v) => Ok(Some(*v)),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::ErrorType.into())
        }
    }

    pub fn set_double(&mut self, index: usize, v: f64) -> Result<()> {
        self.set_value(index, v)
    }

    pub fn get_int(&self, index: usize) -> Result<Option<i32>> {
        let val = self.get(index)?;
        match val {
            Value::INTEGER(v) => Ok(Some(*v)),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::ErrorType.into())
        }
    }

    pub fn set_int(&mut self, index: usize, v: i32) -> Result<()> {
        self.set_value(index, v)
    }

    pub fn get_long(&self, index: usize) -> Result<Option<i64>> {
        let val = self.get(index)?;
        match val {
            Value::LONG(v) => Ok(Some(*v)),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::ErrorType.into())
        }
    }

    pub fn set_long(&mut self, index: usize, v: i64) -> Result<()> {
        self.set_value(index, v)
    }

    pub fn get_datetime(&self, index: usize) -> Result<Option<TimeStamp>> {
        let val = self.get(index)?;
        match val {
            Value::DATETIME(v) => Ok(Some(*v)),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::ErrorType.into())
        }
    }

    pub fn set_datetime(&mut self, index: usize, v: TimeStamp) -> Result<()> {
        self.set_value(index, v)
    }

    pub fn get_bool(&self, index: usize) -> Result<Option<bool>> {
        let val = self.get(index)?;
        match val {
            Value::BOOLEAN(v) => Ok(Some(*v)),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::ErrorType.into())
        }
    }

    pub fn set_bool(&mut self, index: usize, v: bool) -> Result<()> {
        self.set_value(index, v)
    }

    pub fn get_blob(&self, index: usize) -> Result<Option<&[u8]>> {
        let val = self.get(index)?;
        match val {
            Value::BLOB(v) => Ok(Some(v.as_ref())),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::ErrorType.into())
        }
    }

    pub fn set_blob(&mut self, index: usize, v: &[u8]) -> Result<()> {
        self.set_value(index, v)
    }

    pub fn set_blob_vec(&mut self, index: usize, v: Vec<u8>) -> Result<()> {
        self.set_value(index, v)
    }
}

#[cfg(test)]
mod tests {
    use super::Record;
    use super::Value;

    #[test]    
    fn test_value_size() {
        assert_eq!(std::mem::size_of::<Value>(), 16);
    }

    #[test]
    fn test_record_value() {
        let mut record = Record::new(8);

        let blob = vec![1u8, 2, 3];

        record.set_null(0).unwrap();
        record.set_str(1, "Hello").unwrap();
        record.set_double(2, 3.14).unwrap();
        record.set_int(3, 100).unwrap();
        record.set_long(4, 10000).unwrap();
        record.set_datetime(5, 123456789).unwrap();
        record.set_bool(6, true).unwrap();
        record.set_blob(7, blob.as_slice()).unwrap();

        assert!(record.is_null(0).unwrap());
        assert_eq!(None, record.get_int(0).unwrap());
        assert_eq!(Some("Hello"), record.get_str(1).unwrap());
        assert_eq!(Some(3.14), record.get_double(2).unwrap());
        assert_eq!(Some(100), record.get_int(3).unwrap());
        assert_eq!(Some(10000), record.get_long(4).unwrap());
        assert_eq!(Some(123456789), record.get_datetime(5).unwrap());
        assert_eq!(Some(true), record.get_bool(6).unwrap());
        assert_eq!(Some(blob.as_slice()), record.get_blob(7).unwrap());
        assert!(record.is_null(8).is_err());
    }
}