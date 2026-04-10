use crate::storage::Value;

pub fn range_slice(list: &[Value], start: i64, stop: i64) -> Vec<Value> {
    let len = list.len() as i64;
    let start = if start < 0 { len + start } else { start };
    let stop = if stop < 0 { len + stop } else { stop };
    let start = start.max(0) as usize;
    let stop = (stop + 1).min(len) as usize;
    if start >= stop {
        return vec![];
    }
    list[start..stop].to_vec()
}

pub fn zset_range(
    zset: Vec<(f64, String)>,
    start: i64,
    stop: i64,
    with_scores: bool,
) -> Vec<Value> {
    let len = zset.len() as i64;
    let start = start.max(0) as usize;
    let stop = if stop < 0 {
        len as usize
    } else {
        stop as usize
    };

    let mut result = vec![];
    for (i, (score, member)) in zset.into_iter().enumerate() {
        if i >= start && i <= stop {
            result.push(Value::Text(member));
            if with_scores {
                result.push(Value::Float(score));
            }
        }
        if i > stop {
            break;
        }
    }
    result
}

pub fn zset_filter_by_score(
    zset: Vec<(f64, String)>,
    min: f64,
    max: f64,
    with_scores: bool,
) -> Vec<Value> {
    let mut result = vec![];
    for (score, member) in zset {
        if score >= min && score <= max {
            result.push(Value::Text(member));
            if with_scores {
                result.push(Value::Float(score));
            }
        }
    }
    result
}
