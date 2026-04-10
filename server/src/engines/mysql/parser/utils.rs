use crate::engines::mysql::error::{SqlError, SqlResult};
use crate::engines::mysql::parser::Rule;

pub fn expect_identifier(
    pair: Option<pest::iterators::Pair<Rule>>,
    ctx: &str,
) -> SqlResult<String> {
    let p = pair.ok_or_else(|| SqlError::Parse(format!("Missing {}", ctx)))?;
    Ok(p.as_str().trim().to_string())
}
