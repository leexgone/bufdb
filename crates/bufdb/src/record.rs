use crate::error::ErrorKind;
use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Default)]
enum Value {
    #[default]
    NULL,
    STRING(Box<String>),
    DOUBLE(f64),
    INTEGER(i32),
    LONG(i64),
    DATETIME(u64),
    BOOLEAN(bool),
    BLOB(Box<Vec<u8>>)
}

impl Value {
    fn is_null(&self) -> bool {
        self == &Value::NULL
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

    pub fn is_null(&self, index: usize) -> Result<bool> {
        if let Some(val) = self.values.get(index) {
            Ok(val.is_null())
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

    pub fn get_str(&self, index: usize) -> Result<&str> {
        if let Some(val) = self.values.get(index) {
            match val {
                Value::STRING(v) => Ok(v.as_ref()),
                Value::NULL => Err(ErrorKind::NullValue.into()),
                _ => Err(ErrorKind::ErrorType.into())
            }
        } else {
            Err(ErrorKind::OutOfBounds.into())
        }
    }

    pub fn set_str(&mut self, index: usize, str: &str) -> Result<()> {
        if let Some(val) = self.values.get_mut(index) {
            *val = Value::STRING(Box::new(str.into()));
            Ok(())
        } else {
            Err(ErrorKind::OutOfBounds.into())
        }
    }

    pub fn get_double(&self, index: usize) -> Result<f64> {
        if let Some(val) = self.values.get(index) {
            match val {
                Value::DOUBLE(v) => Ok(*v),
                Value::NULL => Err(ErrorKind::NullValue.into()),
                _ => Err(ErrorKind::ErrorType.into())
            }
        } else {
            Err(ErrorKind::OutOfBounds.into())
        }
    }

    pub fn set_double(&mut self, index: usize, v: f64) -> Result<()> {
        if let Some(val) = self.values.get_mut(index) {
            *val = Value::DOUBLE(v);
            Ok(())
        } else {
            Err(ErrorKind::OutOfBounds.into())
        }
    }

    pub fn get_int(&self, index: usize) -> Result<i32> {
        if let Some(val) = self.values.get(index) {
            match val {
                Value::INTEGER(v) => Ok(*v),
                Value::NULL => Err(ErrorKind::NullValue.into()),
                _ => Err(ErrorKind::ErrorType.into())
            }
        } else {
            Err(ErrorKind::OutOfBounds.into())
        }
    }

    pub fn set_int(&mut self, index: usize, v: i32) -> Result<()> {
        if let Some(val) = self.values.get_mut(index) {
            *val = Value::INTEGER(v);
            Ok(())
        } else {
            Err(ErrorKind::OutOfBounds.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Value;

    #[test]    
    fn test_value_size() {
        println!("Size of Value: {}", std::mem::size_of::<Value>())
    }
}