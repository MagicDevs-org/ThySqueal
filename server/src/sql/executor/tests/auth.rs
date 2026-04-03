use crate::sql::Executor;
use crate::sql::executor::Session;
use crate::storage::Database;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_rbac_basic() {
    let db = Arc::new(RwLock::new(Database::new()));
    let executor = Arc::new(Executor::new(db));

    let root_session = Session::new(Some("root".to_string()), None);

    // 1. Root can do everything
    executor
        .execute("CREATE TABLE test (id INT)", vec![], root_session.clone())
        .await
        .unwrap();
    executor
        .execute(
            "CREATE USER 'bob' IDENTIFIED BY 'pass'",
            vec![],
            root_session.clone(),
        )
        .await
        .unwrap();

    // 2. Bob cannot select without permission
    let bob_session = Session::new(Some("bob".to_string()), None);
    let res = executor
        .execute("SELECT * FROM test", vec![], bob_session.clone())
        .await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("does not have Select privilege")
    );

    // 3. Grant permission
    executor
        .execute(
            "GRANT SELECT ON test TO 'bob'",
            vec![],
            root_session.clone(),
        )
        .await
        .unwrap();

    // 4. Bob can now select
    let res = executor
        .execute("SELECT * FROM test", vec![], bob_session.clone())
        .await;
    assert!(res.is_ok());

    // 5. Bob still cannot insert
    let res = executor
        .execute("INSERT INTO test VALUES (1)", vec![], bob_session.clone())
        .await;
    assert!(res.is_err());

    // 6. Grant INSERT globally
    executor
        .execute(
            "GRANT INSERT ON ALL PRIVILEGES TO 'bob'",
            vec![],
            root_session.clone(),
        )
        .await
        .unwrap();
    let res = executor
        .execute("INSERT INTO test VALUES (1)", vec![], bob_session.clone())
        .await;
    assert!(res.is_ok());

    // 7. Revoke
    executor
        .execute(
            "REVOKE SELECT ON test FROM 'bob'",
            vec![],
            root_session.clone(),
        )
        .await
        .unwrap();
    let res = executor
        .execute("SELECT * FROM test", vec![], bob_session.clone())
        .await;
    assert!(res.is_err());
}
