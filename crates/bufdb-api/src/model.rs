use serde::Deserialize;
use serde::Serialize;

use crate::datatype::DataType;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default, Serialize, Deserialize)]
pub enum IndexType {
    #[default]
    NORMAL = 0,
    UNIQUE = 1,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default, Serialize, Deserialize)]
pub enum OrderMode {
    #[default]
    ASC = 0,
    DESC = 1
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FieldDefine {
    pub name: String,
    pub datatype: DataType,
    pub comment: Option<String>
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OrderedField {
    pub field_name: String,
    pub order_mode: OrderMode
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexDefine {
    pub name: String,
    pub index_type: IndexType,
    pub fields: Vec<OrderedField>,
    pub comment: Option<String>
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableDefine {
    pub name: String,
    pub comment: Option<String>,
    pub fields: Vec<FieldDefine>,
    pub key_fields: Vec<String>,
    pub indexes: Vec<IndexDefine>
}