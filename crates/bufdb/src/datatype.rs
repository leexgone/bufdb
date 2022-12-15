use strum::Display;
use strum::EnumString;
use strum::FromRepr;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default, Display, EnumString, FromRepr)]
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
