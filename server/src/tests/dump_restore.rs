use super::common::setup;
use crate::sql::Executor;
use crate::sql::executor::Session;
use crate::storage::{Database, Value};
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_dump_restore() {
    setup();
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE dump_test (id INT, name TEXT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "CREATE UNIQUE INDEX idx_dump_id ON dump_test (id)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO dump_test VALUES (1, 'alice')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO dump_test VALUES (2, 'bob')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    // Dump
    let dump_sql = executor.dump().await.unwrap();
    assert!(dump_sql.contains("CREATE TABLE dump_test"));
    assert!(dump_sql.contains("INSERT INTO dump_test"));
    assert!(dump_sql.contains("CREATE UNIQUE INDEX idx_dump_id"));

    // Restore into a new database
    let db2 = Arc::new(RwLock::new(Database::new()));
    let executor2 = Arc::new(Executor::new(db2));
    executor2
        .execute_batch(&dump_sql, Session::new(None, None))
        .await
        .unwrap();

    // Verify
    let res = executor2
        .execute(
            "SELECT name FROM dump_test WHERE id = 1",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    assert_eq!(res.rows[0][0], Value::Text("alice".to_string()));

    let res = executor2
        .execute(
            "SELECT name FROM dump_test WHERE id = 2",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    assert_eq!(res.rows[0][0], Value::Text("bob".to_string()));

    // Verify index works in restored db
    let res = executor2
        .execute(
            "INSERT INTO dump_test VALUES (1, 'duplicate')",
            vec![],
            Session::new(None, None),
        )
        .await;
    assert!(res.is_err());
}
