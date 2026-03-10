use crate::sql::Executor;
use crate::storage::{Database, Value};
use std::sync::Arc;

#[tokio::test]
async fn test_auto_increment() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    // Test with AUTO_INCREMENT keyword
    executor
        .execute("CREATE TABLE users (id INT AUTO_INCREMENT, name TEXT)", vec![], None)
        .await
        .unwrap();

    // Insert without ID
    executor
        .execute("INSERT INTO users (name) VALUES ('Alice')", vec![], None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users (name) VALUES ('Bob')", vec![], None)
        .await
        .unwrap();

    let result = executor.execute("SELECT * FROM users ORDER BY id", vec![], None).await.unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Text("Alice".to_string()));
    assert_eq!(result.rows[1][0], Value::Int(2));
    assert_eq!(result.rows[1][1], Value::Text("Bob".to_string()));

    // Insert with explicit NULL
    executor
        .execute("INSERT INTO users VALUES (NULL, 'Charlie')", vec![], None)
        .await
        .unwrap();
    
    let result = executor.execute("SELECT * FROM users WHERE name = 'Charlie'", vec![], None).await.unwrap();
    assert_eq!(result.rows[0][0], Value::Int(3));
}

#[tokio::test]
async fn test_serial_shorthand() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    // Test with SERIAL shorthand
    executor
        .execute("CREATE TABLE tasks (id SERIAL, task TEXT)", vec![], None)
        .await
        .unwrap();

    executor
        .execute("INSERT INTO tasks (task) VALUES ('Task 1')", vec![], None)
        .await
        .unwrap();

    let result = executor.execute("SELECT id FROM tasks", vec![], None).await.unwrap();
    assert_eq!(result.rows[0][0], Value::Int(1));
}
