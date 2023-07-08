use std::fmt::Display;

use failure::Context;
use failure::Fail;

/// Enumerates error kinds.
#[derive(Debug, Fail, Default)]
pub enum ErrorKind {
    #[default]
    #[fail(display = "Unknown error")]
    Unknown,
    #[fail(display = "Not found")]
    NotFound,
    #[fail(display = "Error datatype")]
    DataType,
    #[fail(display = "Index out of bounds")]
    OutOfBounds,
    #[fail(display = "Null value")]
    NullValue,
    #[fail(display = "Undefined expression")]
    UndefinedExpr,
    #[fail(display = "Invalidate configuration")]
    Configuration,
    #[fail(display = "Close using object")]
    CloseUsing,
    #[fail(display = "Create duplicate object")]
    CreateDuplicate,
    #[fail(display = "Too many files")]
    TooManyFiles,
    #[fail(display = "Object is already closed")]
    AlreadyClosed,
    #[fail(display = "Format error")]
    Format(#[cause] std::fmt::Error),
    #[fail(display = "Parse float error")]
    ParseFloat(#[cause] std::num::ParseFloatError),
    #[fail(display = "Parse int error")]
    ParseInt(#[cause] std::num::ParseIntError),
    #[fail(display = "Parse bool error")]
    ParseBool(#[cause] std::str::ParseBoolError),
    #[fail(display = "Parse datetime error")]
    ParseDateTime(#[cause] chrono::format::ParseError),
    #[fail(display = "IO error")]
    IO(#[cause] std::io::Error),
    #[fail(display = "JSON error")]
    JSON(serde_json::Error),
    #[fail(display = "database open error")]
    DBOpen(#[cause] PhantomError),
    #[fail(display = "database read error")]
    DBRead(#[cause] PhantomError),
    #[fail(display = "database write error")]
    DBWrite(#[cause] PhantomError),
    #[fail(display = "database close error")]
    DBClose(#[cause] PhantomError),
    #[fail(display = "database error")]
    DBOther(#[cause] PhantomError),
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

impl From<chrono::format::ParseError> for Error {
    fn from(err: chrono::format::ParseError) -> Self {
        ErrorKind::ParseDateTime(err).into()
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        ErrorKind::IO(err).into()
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        ErrorKind::JSON(err).into()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PhantomError {
    message: Option<String>
}

impl PhantomError {
    pub fn from<T: std::error::Error>(err: T) -> Self {
        let message = err.to_string();
        Self { 
            message: if message.is_empty() { None } else { Some(message) }
        }        
    }

    pub fn from_str(msg: &str) -> Self {
        Self {
            message: Some(msg.into())
        }
    }
}

impl Display for PhantomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref msg) = self.message {
            write!(f, "{}", msg)
        } else {
            write!(f, "unknown error")
        }
    }
}

impl std::error::Error for PhantomError {
    fn description(&self) -> &str {
        if let Some(ref msg) = self.message {
            msg.as_ref()
        } else {
            "unknown error"
        }
    }
}

#[macro_export]
macro_rules! db_error {
    (open => $err: expr) => {
        bufdb_lib::error::Error::from(bufdb_lib::error::ErrorKind::DBOpen(bufdb_lib::error::PhantomError::from($err)))
    };
     (read => $err: expr) => {
        bufdb_lib::error::Error::from(bufdb_lib::error::ErrorKind::DBRead(bufdb_lib::error::PhantomError::from($err)))
    };
    (write => $err: expr) => {
        bufdb_lib::error::Error::from(bufdb_lib::error::ErrorKind::DBWrite(bufdb_lib::error::PhantomError::from($err)))
    };
    (close => $err: expr) => {
        bufdb_api::error::Error::from(bufdb_api::error::ErrorKind::DBClose(bufdb_api::error::PhantomError::from($err)))
    };
    ($err: expr) => {
        bufdb_api::error::Error::from(bufdb_api::error::ErrorKind::DBOther(bufdb_api::error::PhantomError::from($err)))
    };
}

#[macro_export]
macro_rules! db_error_s {
    (open => $err: literal) => {
        bufdb_api::error::Error::from(bufdb_api::error::ErrorKind::DBOpen(bufdb_api::error::PhantomError::from_str($err)))
    };
     (read => $err: literal) => {
        bufdb_lib::error::Error::from(bufdb_lib::error::ErrorKind::DBRead(bufdb_lib::error::PhantomError::from_str($err)))
    };
    (write => $err: literal) => {
        bufdb_api::error::Error::from(bufdb_api::error::ErrorKind::DBWrite(bufdb_api::error::PhantomError::from_str($err)))
    };
    (close => $err: literal) => {
        bufdb_api::error::Error::from(bufdb_api::error::ErrorKind::DBClose(bufdb_api::error::PhantomError::from_str($err)))
    };
    ($err: literal) => {
        bufdb_api::error::Error::from(bufdb_api::error::ErrorKind::DBOther(bufdb_api::error::PhantomError::from_str($err)))
    };
}