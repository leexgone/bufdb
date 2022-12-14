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
    use super::Value;

    #[test]    
    fn test_value_size() {
        println!("Size of Value: {}", std::mem::size_of::<Value>());
    }
}