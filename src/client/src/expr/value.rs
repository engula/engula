use engula_apis::*;

#[derive(Debug)]
pub enum Value {
    None,
    Int64(i64),
}

impl Value {
    pub fn as_i64(self) -> Option<i64> {
        if let Value::Int64(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::Int64(v)
    }
}

impl From<GenericValue> for Value {
    fn from(v: GenericValue) -> Self {
        if let Some(v) = v.value {
            match v {
                generic_value::Value::Int64Value(v) => Self::Int64(v),
                _ => todo!(),
            }
        } else {
            Self::None
        }
    }
}

impl From<Option<GenericValue>> for Value {
    fn from(v: Option<GenericValue>) -> Self {
        if let Some(v) = v {
            v.into()
        } else {
            Self::None
        }
    }
}

impl Into<GenericValue> for Value {
    fn into(self) -> GenericValue {
        let value = match self {
            Value::None => None,
            Value::Int64(v) => Some(generic_value::Value::Int64Value(v)),
        };
        GenericValue { value }
    }
}
