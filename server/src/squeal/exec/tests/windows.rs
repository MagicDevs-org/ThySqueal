use crate::squeal::exec::{Executor, Session};
use crate::storage::{Database, Value};
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_window_row_number() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE sales (id INT, name TEXT, amount INT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO sales VALUES (1, 'Alice', 100)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (2, 'Bob', 200)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (3, 'Charlie', 150)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT name, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) as row_num FROM sales ORDER BY amount DESC",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.columns, vec!["name", "amount", "row_num"]);
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], Value::Text("Bob".to_string()));
    assert_eq!(result.rows[0][2], Value::Int(1));
    assert_eq!(result.rows[1][0], Value::Text("Charlie".to_string()));
    assert_eq!(result.rows[1][2], Value::Int(2));
    assert_eq!(result.rows[2][0], Value::Text("Alice".to_string()));
    assert_eq!(result.rows[2][2], Value::Int(3));
}

#[tokio::test]
async fn test_window_rank() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE scores (id INT, name TEXT, score INT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO scores VALUES (1, 'Alice', 100)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO scores VALUES (2, 'Bob', 100)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO scores VALUES (3, 'Charlie', 90)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT name, score, RANK() OVER (ORDER BY score DESC) as rnk FROM scores ORDER BY score DESC",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 3);
    println!("DEBUG RANK: {:?}", result.rows);
    println!("Expected: Alice(100)=1, Bob(100)=1, Charlie(90)=3");
    assert_eq!(result.rows[0][2], Value::Int(1));
    assert_eq!(result.rows[1][2], Value::Int(1));
    assert_eq!(result.rows[2][2], Value::Int(3));
}

#[tokio::test]
async fn test_window_dense_rank() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE scores (id INT, name TEXT, score INT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO scores VALUES (1, 'Alice', 100)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO scores VALUES (2, 'Bob', 100)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO scores VALUES (3, 'Charlie', 90)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) as drnk FROM scores ORDER BY score DESC",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][2], Value::Int(1));
    assert_eq!(result.rows[1][2], Value::Int(1));
    assert_eq!(result.rows[2][2], Value::Int(2));
}

#[tokio::test]
async fn test_window_partition_by() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE sales (id INT, region TEXT, name TEXT, amount INT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO sales VALUES (1, 'East', 'Alice', 100)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (2, 'East', 'Bob', 200)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (3, 'West', 'Charlie', 150)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (4, 'West', 'Dave', 250)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT region, name, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount ASC) as row_num FROM sales",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 4);
    assert_eq!(result.rows[0][0], Value::Text("East".to_string()));
    assert_eq!(result.rows[0][3], Value::Int(1));
    assert_eq!(result.rows[1][0], Value::Text("East".to_string()));
    assert_eq!(result.rows[1][3], Value::Int(2));
    assert_eq!(result.rows[2][0], Value::Text("West".to_string()));
    assert_eq!(result.rows[2][3], Value::Int(1));
    assert_eq!(result.rows[3][0], Value::Text("West".to_string()));
    assert_eq!(result.rows[3][3], Value::Int(2));
}

#[tokio::test]
async fn test_window_ntile() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE scores (id INT, score INT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    for i in 1..=10 {
        executor
            .execute(
                &format!("INSERT INTO scores VALUES ({}, {})", i, i * 10),
                vec![],
                Session::new(None, None),
            )
            .await
            .unwrap();
    }

    let result = executor
        .execute(
            "SELECT id, score, NTILE(4) OVER (ORDER BY id) as quartile FROM scores",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 10);
    assert_eq!(result.rows[0][2], Value::Int(1));
    assert_eq!(result.rows[4][2], Value::Int(2));
    assert_eq!(result.rows[9][2], Value::Int(4));
}

#[tokio::test]
async fn test_window_lag() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE sales (id INT, amount INT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO sales VALUES (1, 100)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (2, 150)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (3, 120)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT id, amount, LAG(amount) OVER (ORDER BY id) as prev_amount FROM sales",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][2], Value::Null);
    assert_eq!(result.rows[1][2], Value::Int(100));
    assert_eq!(result.rows[2][2], Value::Int(150));
}

#[tokio::test]
async fn test_window_lead() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE sales (id INT, amount INT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO sales VALUES (1, 100)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (2, 150)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (3, 120)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT id, amount, LEAD(amount) OVER (ORDER BY id) as next_amount FROM sales",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][2], Value::Int(150));
    assert_eq!(result.rows[1][2], Value::Int(120));
    assert_eq!(result.rows[2][2], Value::Null);
}

#[tokio::test]
async fn test_window_first_value() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE sales (id INT, name TEXT, amount INT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO sales VALUES (1, 'Alice', 100)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (2, 'Bob', 200)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (3, 'Charlie', 150)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT name, amount, FIRST_VALUE(amount) OVER (ORDER BY amount) as min_amount FROM sales",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][2], Value::Int(100));
    assert_eq!(result.rows[1][2], Value::Int(100));
    assert_eq!(result.rows[2][2], Value::Int(100));
}

#[tokio::test]
async fn test_window_last_value() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE sales (id INT, name TEXT, amount INT)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO sales VALUES (1, 'Alice', 100)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (2, 'Bob', 200)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO sales VALUES (3, 'Charlie', 150)",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT name, amount, LAST_VALUE(amount) OVER (ORDER BY amount) as last_val FROM sales ORDER BY amount",
            vec![],
            Session::new(None, None),
        )
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));
    assert_eq!(result.rows[0][2], Value::Int(100));
    assert_eq!(result.rows[1][0], Value::Text("Charlie".to_string()));
    assert_eq!(result.rows[1][2], Value::Int(150));
    assert_eq!(result.rows[2][0], Value::Text("Bob".to_string()));
    assert_eq!(result.rows[2][2], Value::Int(200));
}
