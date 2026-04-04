use super::common::setup;
use crate::config::{Config, LoggingConfig, SecurityConfig, ServerConfig, StorageConfig};
use crate::http::create_app;
use crate::sql::Executor;
use crate::sql::executor::Session;
use crate::storage::Database;
use crate::storage::persistence::SledPersister;

use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::Response,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt; // for `oneshot`

#[tokio::test]
async fn test_sql_lifecycle() {
    setup();
    let temp_dir =
        std::env::temp_dir().join(format!("thysqueal-lifecycle-test-{}", uuid::Uuid::new_v4()));
    let data_dir = temp_dir.to_str().unwrap().to_string();

    let db = Database::with_persister(
        Box::new(SledPersister::new(&data_dir).unwrap()),
        data_dir.clone(),
    )
    .unwrap();
    let db_lock = Arc::new(RwLock::new(db));
    let executor = Arc::new(Executor::new(db_lock).with_data_dir(data_dir.clone()));

    let config = Config {
        server: ServerConfig {
            host: "127.0.0.1".to_string(),
            sql_port: 3306,
            http_port: 9200,
            redis_port: Some(6379),
        },
        storage: StorageConfig {
            max_memory_mb: 1024,
            default_cache_size: 1000,
            default_eviction: "LRU".to_string(),
            snapshot_interval_sec: 300,
            data_dir: data_dir.clone(),
        },
        security: SecurityConfig {
            auth_enabled: false,
            tls_enabled: false,
        },
        logging: LoggingConfig {
            level: "info".to_string(),
        },
    };
    let app = create_app(executor, Arc::new(config));

    // 1. CREATE TABLE
    let response: Response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "CREATE TABLE users (id INT, name TEXT)"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body["success"].as_bool().unwrap());

    // 2. INSERT
    let response: Response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "INSERT INTO users (id, name) VALUES (1, 'alice')"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body["success"].as_bool().unwrap());
    assert_eq!(body["rows_affected"].as_u64().unwrap(), 1);

    // 3. SELECT
    let response: Response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "SELECT * FROM users WHERE name = 'alice'"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body["success"].as_bool().unwrap());
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0][1], "alice");

    // 4. UPDATE
    let response: Response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "UPDATE users SET name = 'bob' WHERE id = 1"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body["success"].as_bool().unwrap());
    assert_eq!(body["rows_affected"].as_u64().unwrap(), 1);

    // 5. SELECT again to verify update
    let response: Response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "SELECT name FROM users WHERE id = 1"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["data"][0][0], "bob");

    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn test_error_handling() {
    setup();
    let db = Database::new();
    let db_lock = Arc::new(RwLock::new(db));
    let executor = Arc::new(Executor::new(db_lock));
    let config = Config::default();
    let app = create_app(executor, Arc::new(config));

    // Table not found error
    let response: Response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "SELECT * FROM non_existent"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(!body["success"].as_bool().unwrap());
    assert!(body["error"].as_str().unwrap().contains("TableNotFound"));
}

#[tokio::test]
async fn test_materialized_views() {
    let db = Database::new();
    let db_lock = Arc::new(RwLock::new(db));
    let executor = Arc::new(Executor::new(db_lock));

    executor
        .execute(
            "CREATE TABLE base (id INT, val INT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO base VALUES (1, 10)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO base VALUES (2, 20)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    // 1. Create MV
    executor
        .execute(
            "CREATE MATERIALIZED VIEW mv_sum AS SELECT SUM(val) FROM base",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let res = executor
        .execute("SELECT * FROM mv_sum", vec![], Session::new(None, None))
        .await
        .unwrap();
    assert_eq!(res.rows[0][0], crate::storage::Value::Int(30));

    // 2. Trigger automatic refresh on INSERT
    executor
        .execute(
            "INSERT INTO base VALUES (3, 30)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    let res = executor
        .execute("SELECT * FROM mv_sum", vec![], Session::new(None, None))
        .await
        .unwrap();
    assert_eq!(res.rows[0][0], crate::storage::Value::Int(60));

    // 3. Trigger automatic refresh on DELETE
    executor
        .execute(
            "DELETE FROM base WHERE id = 1",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    let res = executor
        .execute("SELECT * FROM mv_sum", vec![], Session::new(None, None))
        .await
        .unwrap();
    assert_eq!(res.rows[0][0], crate::storage::Value::Int(50));
}
