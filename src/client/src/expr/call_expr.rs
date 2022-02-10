use engula_apis::*;

pub fn get() -> CallExpr {
    CallExpr {
        function: Some(call_expr::Function::Generic(GenericFunction::Get as i32)),
        ..Default::default()
    }
}

pub fn set(v: impl Into<GenericValue>) -> CallExpr {
    CallExpr {
        function: Some(call_expr::Function::Generic(GenericFunction::Set as i32)),
        arguments: vec![v.into()],
    }
}

pub fn delete() -> CallExpr {
    CallExpr {
        function: Some(call_expr::Function::Generic(GenericFunction::Delete as i32)),
        ..Default::default()
    }
}
