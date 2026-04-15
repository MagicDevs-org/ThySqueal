use crate::storage::Value;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Session {
    pub username: String,
    pub database: Option<String>,
    pub transaction_id: Option<String>,
    pub variables: HashMap<String, Value>,
}

impl Session {
    pub fn new(username: Option<String>, transaction_id: Option<String>) -> Self {
        Self {
            username: username.unwrap_or_else(|| "root".to_string()),
            database: None,
            transaction_id,
            variables: HashMap::new(),
        }
    }

    #[allow(unused)]
    pub fn with_database(mut self, database: Option<String>) -> Self {
        self.database = database;
        self
    }

    #[allow(dead_code)]
    pub fn with_variables(
        username: Option<String>,
        transaction_id: Option<String>,
        variables: HashMap<String, Value>,
    ) -> Self {
        Self {
            username: username.unwrap_or_else(|| "root".to_string()),
            database: None,
            transaction_id,
            variables,
        }
    }

    pub fn root() -> Self {
        Self::new(None, None)
    }
}

pub struct ExecutionContext {
    pub params: Vec<Value>,
    pub session: Session,
}

impl ExecutionContext {
    pub fn new(params: Vec<Value>, session: Session) -> Self {
        Self { params, session }
    }
}
