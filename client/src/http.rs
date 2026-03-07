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
    pub error: Option<serde_json::Value>,
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
        if !result.data.is_empty() {
            println!("{}", serde_json::to_string_pretty(&result.data)?);
        } else {
            println!("Success. Rows affected: {}", result.rows_affected);
        }
    } else if let Some(error) = result.error {
        if let Some(err_type) = error.get("type").and_then(|v| v.as_str()) {
            let details = error.get("details").and_then(|v| v.as_str()).unwrap_or("");
            eprintln!("Error ({}): {}", err_type, details);
        } else {
            eprintln!("Error: {}", error);
        }
    }

    Ok(())
}
