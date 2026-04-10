use super::common::setup;
use crate::config::Config;
use crate::http::create_app;
use crate::squeal::exec::Executor;
use crate::storage::Database;

use axum::{body::Body, http::Request};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt; // for `oneshot`

#[tokio::test]
async fn test_transactions() {
    setup();
    let db = Database::new();
    let db_lock = Arc::new(RwLock::new(db));
    let executor = Arc::new(Executor::new(db_lock));
    let config = Config::default();
    let app = create_app(executor, Arc::new(config));

    // 1. Create table
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "CREATE TABLE accounts (id INT, balance FLOAT)"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // 2. BEGIN
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(json!({"sql": "BEGIN"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    let tx_id = body["transaction_id"].as_str().unwrap().to_string();

    // 3. INSERT in TX
    app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/_query")
            .header("Content-Type", "application/json")
            .body(Body::from(json!({"sql": "INSERT INTO accounts VALUES (1, 100.0)", "transaction_id": tx_id}).to_string()))
            .unwrap(),
    ).await.unwrap();

    // 4. Verify NOT visible globally
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT * FROM accounts"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body["data"].as_array().unwrap().is_empty());

    // 5. COMMIT
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "COMMIT", "transaction_id": tx_id}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // 6. Verify visible globally
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT * FROM accounts"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_rollback() {
    setup();
    let db = Database::new();
    let db_lock = Arc::new(RwLock::new(db));
    let executor = Arc::new(Executor::new(db_lock));
    let config = Config::default();
    let app = create_app(executor, Arc::new(config));

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "CREATE TABLE t (id INT)"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(json!({"sql": "BEGIN"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    let tx_id = body["transaction_id"].as_str().unwrap().to_string();

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "INSERT INTO t VALUES (1)", "transaction_id": tx_id}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "ROLLBACK", "transaction_id": tx_id}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(json!({"sql": "SELECT * FROM t"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body["data"].as_array().unwrap().is_empty());
}
