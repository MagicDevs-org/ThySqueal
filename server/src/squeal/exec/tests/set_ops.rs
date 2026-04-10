use crate::squeal::exec::{Executor, Session};
use crate::storage::{Database, Value};
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_union() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE t1 (id INT, name TEXT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO t1 VALUES (1, 'a')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO t1 VALUES (2, 'b')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "CREATE TABLE t2 (id INT, name TEXT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO t2 VALUES (3, 'c')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO t2 VALUES (4, 'd')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT name FROM t1 UNION SELECT name FROM t2",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 4);
    let names: Vec<_> = result.rows.iter().map(|r| &r[0]).collect();
    assert!(names.contains(&&Value::Text("a".to_string())));
    assert!(names.contains(&&Value::Text("b".to_string())));
    assert!(names.contains(&&Value::Text("c".to_string())));
    assert!(names.contains(&&Value::Text("d".to_string())));
}

#[tokio::test]
async fn test_union_all() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE t1 (id INT, name TEXT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO t1 VALUES (1, 'a')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "CREATE TABLE t2 (id INT, name TEXT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO t2 VALUES (1, 'a')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT name FROM t1 UNION ALL SELECT name FROM t2",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Text("a".to_string()));
    assert_eq!(result.rows[1][0], Value::Text("a".to_string()));
}

#[tokio::test]
async fn test_union_removes_duplicates() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE t1 (id INT, name TEXT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO t1 VALUES (1, 'a')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "CREATE TABLE t2 (id INT, name TEXT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO t2 VALUES (1, 'a')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT name FROM t1 UNION SELECT name FROM t2",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Text("a".to_string()));
}

#[tokio::test]
async fn test_intersect() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE t1 (id INT, name TEXT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO t1 VALUES (1, 'a')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO t1 VALUES (2, 'b')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "CREATE TABLE t2 (id INT, name TEXT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO t2 VALUES (1, 'a')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO t2 VALUES (2, 'c')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT name FROM t1 INTERSECT SELECT name FROM t2",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Text("a".to_string()));
}

#[tokio::test]
async fn test_except() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE t1 (id INT, name TEXT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO t1 VALUES (1, 'a')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO t1 VALUES (2, 'b')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "CREATE TABLE t2 (id INT, name TEXT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO t2 VALUES (1, 'a')",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT name FROM t1 EXCEPT SELECT name FROM t2",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Text("b".to_string()));
}

#[tokio::test]
async fn test_multiple_set_operations() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    for i in 1..=4 {
        executor
            .execute(
                &format!("CREATE TABLE t{} (id INT, name TEXT)", i),
                vec![],
                Session::new(None, None),
            )
            .await
            .unwrap();

        executor
            .execute(
                &format!(
                    "INSERT INTO t{} VALUES ({}, '{}')",
                    i,
                    i,
                    (b'a' + i as u8) as char
                ),
                vec![],
                Session::new(None, None),
            )
            .await
            .unwrap();
    }

    let result = executor
        .execute(
            "SELECT name FROM t1 UNION SELECT name FROM t2 UNION SELECT name FROM t3",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 3);
}
