use std::fmt::Display;

use failure::Context;
use failure::Fail;

/// Enumerates error kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Fail)]
pub enum ErrorKind {
    #[fail(display = "Error Data Type")]
    ErrorType,
    #[fail(display = "Index out of bounds")]
    OutOfBounds
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

impl Error {
    pub fn kind(&self) -> ErrorKind {
        *self.inner.get_context()
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { inner: Context::new(kind) }
    }
}

pub type Result<T> = core::result::Result<T, Error>;