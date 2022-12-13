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

    pub fn get_str(&self, index: usize) -> Result<Option<&str>> {
         let val = self.get(index)?;
        match val {
            Value::STRING(v) => Ok(Some(v.as_ref())),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::ErrorType.into())
        }
    }

    pub fn set_str(&mut self, index: usize, str: &str) -> Result<()> {
        let val = self.get_mut(index)?;
        *val = Value::STRING(Box::new(str.into()));
        Ok(())
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
        let val = self.get_mut(index)?;
        *val = Value::DOUBLE(v);
        Ok(())
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

    pub fn get_long(&self, index: usize) -> Result<Option<i64>> {
        if let Some(val) = self.values.get(index) {
            match val {
                Value::LONG(v) => Ok(Some(*v)),
                Value::NULL => Ok(None),
                _ => Err(ErrorKind::ErrorType.into())
            }
        } else {
            Err(ErrorKind::OutOfBounds.into())
        }
    }

    pub fn set_long(&mut self, index: usize, v: i64) -> Result<()> {
        if let Some(val) = self.values.get_mut(index) {
            *val = Value::LONG(v);
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