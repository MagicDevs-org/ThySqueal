use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    pub success: bool,
    #[serde(default)]
    pub data: Vec<serde_json::Value>,
    #[serde(default)]
    pub rows_affected: u64,
    #[serde(default)]
    pub execution_time_ms: u64,
    #[serde(default)]
    pub error: Option<QueryError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryError {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub position: Option<usize>,
}

pub async fn execute_query(host: &str, port: u16, sql: &str) -> Result<()> {
    let url = format!("http://{}:{}/_query", host, port);
    
    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .json(&QueryRequest {
            sql: sql.to_string(),
            params: vec![],
        })
        .send()
        .await?;

    let result: QueryResponse = response.json().await?;
    
    if result.success {
        println!("{}", serde_json::to_string_pretty(&result.data)?);
    } else if let Some(error) = result.error {
        eprintln!("Error: {} - {}", error.code, error.message);
    }

    Ok(())
}
