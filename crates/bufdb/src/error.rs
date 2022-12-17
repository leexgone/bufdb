use std::fmt::Display;

use failure::Context;
use failure::Fail;

/// Enumerates error kinds.
#[derive(Debug, Clone, PartialEq, Eq, Fail, Default)]
pub enum ErrorKind {
    #[default]
    #[fail(display = "Unknown error")]
    Unknown,
    #[fail(display = "Error datatype")]
    DataType,
    #[fail(display = "Index out of bounds")]
    OutOfBounds,
    #[fail(display = "Null value")]
    NullValue,
    #[fail(display = "Undefined expression")]
    UndefinedExpr,
    #[fail(display = "Format error")]
    Format(#[cause] std::fmt::Error),
    #[fail(display = "Parse float error")]
    ParseFloat(#[cause] std::num::ParseFloatError),
    #[fail(display = "Parse int error")]
    ParseInt(#[cause] std::num::ParseIntError),
    #[fail(display = "Parse bool error")]
    ParseBool(#[cause] std::str::ParseBoolError),
}

/// Defines error type for bufdb lib.
#[derive(Debug)]
pub struct Error {
    inner: Context<ErrorKind>
}

impl Fail for Error {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&failure::Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl Default for Error {
    fn default() -> Self {
        Self { 
            inner: Context::new(Default::default()) 
        }
    }
}

impl Error {
    pub fn new_datatype_err() -> Self {
        Self { 
            inner: Context::new(ErrorKind::DataType) 
        }
    }
    pub fn kind(&self) -> &ErrorKind {
        self.inner.get_context()
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { inner: Context::new(kind) }
    }
}

pub type Result<T> = core::result::Result<T, Error>;

impl From<std::fmt::Error> for Error {
    fn from(err: std::fmt::Error) -> Self {
        ErrorKind::Format(err).into()
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(err: std::num::ParseFloatError) -> Self {
        ErrorKind::ParseFloat(err).into()
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        ErrorKind::ParseInt(err).into()
    }
}

impl From<std::str::ParseBoolError> for Error {
    fn from(err: std::str::ParseBoolError) -> Self {
        ErrorKind::ParseBool(err).into()
    }
}