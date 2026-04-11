use crate::squeal::exec::{ExecError, ExecResult};
use crate::squeal::ir::ScalarFuncType;
use crate::storage::Value;
use sha2::Digest;

pub fn evaluate_scalar_func(name: &ScalarFuncType, args: &[Value]) -> ExecResult<Value> {
    match name {
        ScalarFuncType::Lower => {
            let val = args
                .first()
                .ok_or_else(|| ExecError::Runtime("LOWER requires 1 argument".to_string()))?;
            let s = val
                .as_text()
                .ok_or_else(|| ExecError::TypeMismatch("LOWER requires text".to_string()))?;
            Ok(Value::Text(s.to_lowercase()))
        }
        ScalarFuncType::Upper => {
            let val = args
                .first()
                .ok_or_else(|| ExecError::Runtime("UPPER requires 1 argument".to_string()))?;
            let s = val
                .as_text()
                .ok_or_else(|| ExecError::TypeMismatch("UPPER requires text".to_string()))?;
            Ok(Value::Text(s.to_uppercase()))
        }
        ScalarFuncType::Length => {
            let val = args
                .first()
                .ok_or_else(|| ExecError::Runtime("LENGTH requires 1 argument".to_string()))?;
            let s = val
                .as_text()
                .ok_or_else(|| ExecError::TypeMismatch("LENGTH requires text".to_string()))?;
            Ok(Value::Int(s.len() as i64))
        }
        ScalarFuncType::Abs => {
            let val = args
                .first()
                .ok_or_else(|| ExecError::Runtime("ABS requires 1 argument".to_string()))?;
            match val {
                Value::Int(i) => Ok(Value::Int(i.abs())),
                Value::Float(f) => Ok(Value::Float(f.abs())),
                _ => Err(ExecError::TypeMismatch(
                    "ABS requires numeric value".to_string(),
                )),
            }
        }
        ScalarFuncType::Now => Ok(Value::DateTime(chrono::Utc::now())),
        ScalarFuncType::Concat => {
            let mut result = String::new();
            for arg in args {
                result.push_str(&arg.to_string_repr());
            }
            Ok(Value::Text(result))
        }
        ScalarFuncType::Coalesce => {
            for arg in args {
                if !matches!(arg, Value::Null) {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        ScalarFuncType::Replace => {
            if args.len() != 3 {
                return Err(ExecError::Runtime(
                    "REPLACE requires 3 arguments".to_string(),
                ));
            }
            let s = args[0].as_text().ok_or_else(|| {
                ExecError::TypeMismatch("REPLACE first arg must be text".to_string())
            })?;
            let from = args[1].as_text().ok_or_else(|| {
                ExecError::TypeMismatch("REPLACE second arg must be text".to_string())
            })?;
            let to = args[2].as_text().ok_or_else(|| {
                ExecError::TypeMismatch("REPLACE third arg must be text".to_string())
            })?;
            Ok(Value::Text(s.replace(from, to)))
        }
        ScalarFuncType::IfNull => {
            if args.len() != 2 {
                return Err(ExecError::Runtime(
                    "IFNULL requires 2 arguments".to_string(),
                ));
            }
            if !matches!(args[0], Value::Null) {
                Ok(args[0].clone())
            } else {
                Ok(args[1].clone())
            }
        }
        ScalarFuncType::If => {
            if args.len() != 3 {
                return Err(ExecError::Runtime("IF requires 3 arguments".to_string()));
            }
            let cond = match &args[0] {
                Value::Bool(b) => *b,
                Value::Null => false,
                _ => {
                    return Err(ExecError::TypeMismatch(
                        "IF condition must be boolean".to_string(),
                    ));
                }
            };
            if cond {
                Ok(args[1].clone())
            } else {
                Ok(args[2].clone())
            }
        }
        ScalarFuncType::DateDiff => {
            if args.len() != 2 {
                return Err(ExecError::Runtime(
                    "DATEDIFF requires 2 arguments".to_string(),
                ));
            }
            use chrono::NaiveDate;
            let date1 = match &args[0] {
                Value::DateTime(dt) => dt.naive_utc().date(),
                Value::Text(s) => {
                    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        d
                    } else {
                        return Err(ExecError::Runtime(
                            "DATEDIFF: invalid date format, expected YYYY-MM-DD".to_string(),
                        ));
                    }
                }
                _ => {
                    return Err(ExecError::TypeMismatch(
                        "DATEDIFF requires date values".to_string(),
                    ));
                }
            };
            let date2 = match &args[1] {
                Value::DateTime(dt) => dt.naive_utc().date(),
                Value::Text(s) => {
                    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        d
                    } else {
                        return Err(ExecError::Runtime(
                            "DATEDIFF: invalid date format, expected YYYY-MM-DD".to_string(),
                        ));
                    }
                }
                _ => {
                    return Err(ExecError::TypeMismatch(
                        "DATEDIFF requires date values".to_string(),
                    ));
                }
            };
            let diff = date1.signed_duration_since(date2);
            Ok(Value::Int(diff.num_days()))
        }
        ScalarFuncType::DateFormat => {
            if args.len() != 2 {
                return Err(ExecError::Runtime(
                    "DATE_FORMAT requires 2 arguments".to_string(),
                ));
            }
            let dt = match &args[0] {
                Value::DateTime(d) => *d,
                _ => {
                    return Err(ExecError::TypeMismatch(
                        "DATE_FORMAT requires datetime".to_string(),
                    ));
                }
            };
            let fmt = args[1].as_text().ok_or_else(|| {
                ExecError::TypeMismatch("DATE_FORMAT format must be text".to_string())
            })?;
            let formatted = dt.format(fmt).to_string();
            Ok(Value::Text(formatted))
        }
        ScalarFuncType::Md5 => {
            let input = args
                .first()
                .ok_or_else(|| ExecError::Runtime("MD5 requires 1 argument".to_string()))?;
            let s = input
                .as_text()
                .ok_or_else(|| ExecError::TypeMismatch("MD5 requires text argument".to_string()))?;
            Ok(Value::Text(format!("{:032x}", md5::compute(s.as_bytes()))))
        }
        ScalarFuncType::Sha2 => {
            let input = args
                .first()
                .ok_or_else(|| ExecError::Runtime("SHA2 requires 1 argument".to_string()))?;
            let s = input.as_text().ok_or_else(|| {
                ExecError::TypeMismatch("SHA2 requires text argument".to_string())
            })?;
            Ok(Value::Text(format!(
                "{:064x}",
                sha2::Sha256::digest(s.as_bytes())
            )))
        }
    }
}
