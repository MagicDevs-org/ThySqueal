use super::common::setup;
use crate::config::Config;
use crate::http::create_app;
use crate::squeal::exec::{Executor, Session};
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
use tower::ServiceExt;

#[tokio::test]
async fn test_search_basic() {
    setup();
    let temp_dir =
        std::env::temp_dir().join(format!("thysqueal-search-test-{}", uuid::Uuid::new_v4()));
    let data_dir = temp_dir.to_str().unwrap().to_string();

    let db = Database::with_persister(
        Box::new(SledPersister::new(&data_dir).unwrap()),
        data_dir.clone(),
    )
    .unwrap();
    let db_lock = Arc::new(RwLock::new(db));
    let executor = Arc::new(Executor::new(db_lock).with_data_dir(data_dir.clone()));

    let mut config = Config::default();
    config.server.host = "127.0.0.1".to_string();
    config.server.http.port = Some(8888);
    config.server.mysql.port = Some(13306);
    config.server.redis.port = Some(16379);
    config.storage.data_dir = data_dir;

    let app = create_app(executor, Arc::new(config));

    let response: Response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "SELECT 1"
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

    let _ = std::fs::remove_dir_all(temp_dir);
}
