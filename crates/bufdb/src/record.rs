use std::ops::Index;
use std::ops::IndexMut;

use crate::datatype::ConvertTo;
use crate::datatype::TimeStamp;
use crate::datatype::Value;
use crate::error::ErrorKind;
use crate::error::Result;

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

    pub fn set_value<T>(&mut self, index: usize, v: T) -> Result<()> where T: Into<Value> {
        let val = self.get_mut(index)?;
        *val = v.into();
        Ok(())
    }

    pub fn get_str(&self, index: usize) -> Result<Option<&str>> {
        let val = self.get(index)?;
        match val {
            Value::STRING(v) => Ok(Some(v.as_ref())),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::DataType.into())
        }
    }

    pub fn as_string(&self, index: usize) -> Result<Option<String>> {
        let val = self.get(index)?;
        val.convert_to()
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
            _ => Err(ErrorKind::DataType.into())
        }
    }

    pub fn as_double(&self, index: usize) -> Result<Option<f64>> {
        let val = self.get(index)?;
        val.convert_to()
    }

    pub fn set_double(&mut self, index: usize, v: f64) -> Result<()> {
        self.set_value(index, v)
    }

    pub fn get_int(&self, index: usize) -> Result<Option<i32>> {
        let val = self.get(index)?;
        match val {
            Value::INT(v) => Ok(Some(*v)),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::DataType.into())
        }
    }

    pub fn as_int(&self, index: usize) -> Result<Option<i32>> {
        let val = self.get(index)?;
        val.convert_to()
    }

    pub fn set_int(&mut self, index: usize, v: i32) -> Result<()> {
        self.set_value(index, v)
    }

    pub fn get_long(&self, index: usize) -> Result<Option<i64>> {
        let val = self.get(index)?;
        match val {
            Value::LONG(v) => Ok(Some(*v)),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::DataType.into())
        }
    }

    pub fn as_long(&self, index: usize) -> Result<Option<i64>> {
        let val = self.get(index)?;
        val.convert_to()
    }

    pub fn set_long(&mut self, index: usize, v: i64) -> Result<()> {
        self.set_value(index, v)
    }

    pub fn get_datetime(&self, index: usize) -> Result<Option<TimeStamp>> {
        let val = self.get(index)?;
        match val {
            Value::DATETIME(v) => Ok(Some(*v)),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::DataType.into())
        }
    }

    pub fn as_datetime(&self, index: usize) -> Result<Option<TimeStamp>> {
        let val = self.get(index)?;
        val.convert_to()
    }

    pub fn set_datetime(&mut self, index: usize, v: TimeStamp) -> Result<()> {
        self.set_value(index, v)
    }

    pub fn get_bool(&self, index: usize) -> Result<Option<bool>> {
        let val = self.get(index)?;
        match val {
            Value::BOOL(v) => Ok(Some(*v)),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::DataType.into())
        }
    }

    pub fn as_bool(&self, index: usize) -> Result<Option<bool>> {
        let val = self.get(index)?;
        val.convert_to()
    }

    pub fn set_bool(&mut self, index: usize, v: bool) -> Result<()> {
        self.set_value(index, v)
    }

    pub fn get_blob(&self, index: usize) -> Result<Option<&[u8]>> {
        let val = self.get(index)?;
        match val {
            Value::BLOB(v) => Ok(Some(v.as_ref())),
            Value::NULL => Ok(None),
            _ => Err(ErrorKind::DataType.into())
        }
    }

    pub fn set_blob(&mut self, index: usize, v: &[u8]) -> Result<()> {
        self.set_value(index, v)
    }

    pub fn set_blob_vec(&mut self, index: usize, v: Vec<u8>) -> Result<()> {
        self.set_value(index, v)
    }
}

impl Index<usize> for Record {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl IndexMut<usize> for Record {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.values[index]
    }
}

impl IntoIterator for Record {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<'a> IntoIterator for &'a Record {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        (&self.values).into_iter()
    }
}

impl<'a> IntoIterator for &'a mut Record {
    type Item = &'a mut Value;
    type IntoIter = std::slice::IterMut<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        (&mut self.values).into_iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::datatype::ConvertTo;
    use crate::datatype::Value;

    use super::Record;

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

    #[test]
    fn test_record_index() {
        let mut record = Record::new(2);

        record[0] = Value::INT(10);
        record[1] = Value::DOUBLE(100f64);

        assert_eq!(record[0], Value::INT(10));
        assert_eq!(record[1], Value::DOUBLE(100f64));
    }

    #[test]
    fn test_record_iter() {
        let mut record = Record::new(3);

        record[0] = 1i32.into();
        record[1] = 10i32.into();
        record[2] = 100i32.into();

        let mut all = 0i32;
        for val in record {
            let v: i32 = val.convert_to().unwrap().unwrap();
            all = all + v;
        }

        assert_eq!(111i32, all);
    }
}