use std::fmt::Display;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum DataType {
    #[default]
    STRING = 1,
    DOUBLE = 2,
    INTEGER = 3,
    LONG = 4,
    DATETIME = 5,
    BOOLEAN = 6,
    BLOB = 7,
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::STRING => f.write_str("string"),
            Self::DOUBLE => f.write_str("double"),
            Self::INTEGER => f.write_str("integer"),
            Self::LONG => f.write_str("long"),
            Self::DATETIME => f.write_str("datetime"),
            Self::BOOLEAN => f.write_str("boolean"),
            Self::BLOB => f.write_str("blob")
        }
    }
}