use crate::squeal::exec::{ExecError, ExecResult};
use crate::storage::{DatabaseState, Privilege};

pub fn check_privilege(
    username: &str,
    table: Option<&str>,
    privilege: Privilege,
    db_state: &DatabaseState,
) -> ExecResult<()> {
    let user = db_state
        .users
        .get(username)
        .ok_or_else(|| ExecError::Runtime(format!("User {} not found", username)))?;

    if user.global_privileges.contains(&Privilege::All) {
        return Ok(());
    }

    if let Some(t) = table
        && let Some(privs) = user.table_privileges.get(t)
        && (privs.contains(&Privilege::All) || privs.contains(&privilege))
    {
        return Ok(());
    }

    if user.global_privileges.contains(&privilege) {
        return Ok(());
    }

    Err(ExecError::PermissionDenied(format!(
        "User {} does not have {:?} privilege{}",
        username,
        privilege,
        table
            .map(|t| format!(" on table {}", t))
            .unwrap_or_default()
    )))
}
