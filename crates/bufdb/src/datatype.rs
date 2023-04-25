use std::fmt::Display;
use std::fmt::Write;
use std::str::FromStr;

use chrono::NaiveDateTime;
use serde::Deserialize;
use serde::Serialize;
use strum::Display;
use strum::EnumString;
use strum::FromRepr;

use crate::error::Error;
use crate::error::Result;

/// Defines supported datatypes in bufdb.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default, Display, EnumString, FromRepr, Serialize, Deserialize)]
pub enum DataType {
    #[default]
    #[strum(serialize = "string")]
    STRING = 1,
    #[strum(serialize = "double")]
    DOUBLE = 2,
    #[strum(serialize = "int")]
    INT = 3,
    #[strum(serialize = "long")]
    LONG = 4,
    #[strum(serialize = "datetime")]
    DATETIME = 5,
    #[strum(serialize = "bool")]
    BOOL = 6,
    #[strum(serialize = "blob")]
    BLOB = 7,
}

/// Defines `TimeStamp` type to store datetime. 
/// 
/// `TimeStamp` stores the number of non-leap seconds since January 1, 1970 0:00:00 UTC (also known as “UNIX timestamp”).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimeStamp(i64);

impl Into<i64> for TimeStamp {
    fn into(self) -> i64 {
        self.0
    }
}

impl From<i64> for TimeStamp {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl Into<NaiveDateTime> for TimeStamp {
    fn into(self) -> NaiveDateTime {
        NaiveDateTime::from_timestamp_millis(self.0).unwrap()
    }
}

impl From<NaiveDateTime> for TimeStamp {
    fn from(value: NaiveDateTime) -> Self {
        Self(value.timestamp_millis())
    }
}

impl Display for TimeStamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(dt) = NaiveDateTime::from_timestamp_millis(self.0) {
            write!(f, "{}", dt)
        } else {
            Err(std::fmt::Error {})
        }
    }
}

/// Defines `Value` object to store values.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Value {
    #[default]
    NULL,
    STRING(Box<String>),
    DOUBLE(f64),
    INT(i32),
    LONG(i64),
    DATETIME(TimeStamp),
    BOOL(bool),
    BLOB(Box<Vec<u8>>)
}

impl Value {
    pub fn is_null(&self) -> bool {
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
        Self::INT(val)
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
        Self::BOOL(val)
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

impl<T> From<Option<T>> for Value where T: Into<Value> {
    fn from(value: Option<T>) -> Self {
        if let Some(v) = value {
            v.into()
        } else {
            Value::NULL
        }
    }
}

macro_rules! to_hex_string {
    ($arr:expr) => {
        {
            let mut s = String::with_capacity($arr.len() * 2);
            for b in $arr.iter() {
                write!(s, "{:02X}", b)?;
            }
            s
        }
    };
    ($f:expr, $arr:expr) => {
        {
            for b in $arr.iter() {
                write!($f, "{:02X}", b)?;
            }
            Ok(())
        }
    };
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NULL => write!(f, "<null>"),
            Self::STRING(v) => write!(f, "\"{}\"", v),
            Self::DOUBLE(v) => write!(f, "{}", v),
            Self::INT(v) => write!(f, "{}", v),
            Self::LONG(v) => write!(f, "{}", v),
            Self::DATETIME(v) => write!(f, "{}", v),
            Self::BOOL(v) => write!(f, "{}", v),
            Self::BLOB(v) => to_hex_string!(f, v),
        }
    }
}

/// `Converter<T>` trait can convert the bufdb `Value` to rust raw type `<T>`.
pub trait ConvertTo<T> {
    /// Converts the current value to type `<T>`. Returns `None` if value is null.
    fn convert_to(&self) -> Result<Option<T>>;
}

impl ConvertTo<String> for Value {
    fn convert_to(&self) -> Result<Option<String>> {
        match self {
            Self::NULL => Ok(None),
            Self::STRING(v) => Ok(Some(v.as_ref().clone())),
            Self::DOUBLE(v) => Ok(Some(v.to_string())),
            Self::INT(v) => Ok(Some(v.to_string())),
            Self::LONG(v) => Ok(Some(v.to_string())),
            Self::DATETIME(v) => Ok(Some(v.to_string())),
            Self::BOOL(v) => Ok(Some(v.to_string())),
            Self::BLOB(v) => Ok(Some(to_hex_string!(v)))
        }
    }
}

impl ConvertTo<f64> for Value {
    fn convert_to(&self) -> Result<Option<f64>> {
        match self {
            Self::NULL => Ok(None),
            Self::STRING(v) => Ok(Some(v.parse()?)),
            Self::DOUBLE(v) => Ok(Some(*v)),
            Self::INT(v) => Ok(Some(*v as _)),
            Self::LONG(v) => Ok(Some(*v as _)),
            Self::DATETIME(v) => Ok(Some(v.0 as _)),
            Self::BOOL(v) => Ok(Some(if *v { 1f64 } else { 0f64 })),
            _ => Err(Error::new_datatype_err())
        }
    }
}

impl ConvertTo<i32> for Value {
    fn convert_to(&self) -> Result<Option<i32>> {
        match self {
            Self::NULL => Ok(None),
            Self::STRING(v) => Ok(Some(v.parse()?)),
            Self::DOUBLE(v) => Ok(Some(*v as _)),
            Self::INT(v) => Ok(Some(*v)),
            Self::LONG(v) => Ok(Some(*v as _)),
            Self::DATETIME(v) => Ok(Some(v.0 as _)),
            Self::BOOL(v) => Ok(Some(*v as _)),
            _ => Err(Error::new_datatype_err())
        }
    }
}

impl ConvertTo<i64> for Value {
    fn convert_to(&self) -> Result<Option<i64>> {
        match self {
            Self::NULL => Ok(None),
            Self::STRING(v) => Ok(Some(v.parse()?)),
            Self::DOUBLE(v) => Ok(Some(*v as _)),
            Self::INT(v) => Ok(Some(*v as _)),
            Self::LONG(v) => Ok(Some(*v)),
            Self::DATETIME(v) => Ok(Some(v.0 as _)),
            Self::BOOL(v) => Ok(Some(*v as _)),
            _ => Err(Error::new_datatype_err())
        }
    }
}

impl ConvertTo<TimeStamp> for Value {
    fn convert_to(&self) -> Result<Option<TimeStamp>> {
        match self {
            Self::NULL => Ok(None),
            Self::STRING(v) => {
                if v.is_empty() {
                    Ok(None)
                } else {
                    let dt = NaiveDateTime::from_str(v)?;
                    Ok(Some(dt.into()))
                }
            },
            Self::DOUBLE(v) => Ok(Some(TimeStamp(*v as _))),
            Self::INT(v) => Ok(Some(TimeStamp(*v as _))),
            Self::LONG(v) => Ok(Some(TimeStamp(*v as _))),
            Self::DATETIME(v) => Ok(Some(*v)),
            Self::BOOL(v) => Ok(Some(TimeStamp(*v as _))),
            _ => Err(Error::new_datatype_err())
        }
    }
}

impl ConvertTo<bool> for Value {
    fn convert_to(&self) -> Result<Option<bool>> {
        match self {
            Self::NULL => Ok(None),
            Self::STRING(v) => Ok(Some(v.parse()?)),
            Self::DOUBLE(v) => Ok(Some(*v != 0f64)),
            Self::INT(v) => Ok(Some(*v != 0)),
            Self::LONG(v) => Ok(Some(*v != 0)),
            Self::DATETIME(v) => Ok(Some(v.0 != 0)),
            Self::BOOL(v) => Ok(Some(*v)),
            _ => Err(Error::new_datatype_err())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::datatype::Value;

    #[test]    
    fn test_value_size() {
        // println!("size of Vec<u8> = {}", std::mem::size_of::<Vec<u8>>());
        // println!("size of Box<Vec<u8>> = {}", std::mem::size_of::<Box<Vec<u8>>>());
        // println!("size of Option<Vec<u8>> = {}", std::mem::size_of::<Option<Vec<u8>>>());

        // println!("size of String = {}", std::mem::size_of::<String>());
        // println!("size of Box<String> = {}", std::mem::size_of::<Box<String>>());
        // println!("size of Option<String> = {}", std::mem::size_of::<Option<String>>());

        assert_eq!(std::mem::size_of::<Value>(), 16);
    }
}