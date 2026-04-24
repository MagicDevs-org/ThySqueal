use crate::storage::{Column, DataType, DatabaseState, Row, Table, Value};
use std::collections::HashMap;

pub fn get_info_schema_tables(db_state: &DatabaseState) -> HashMap<String, Table> {
    let mut tables = HashMap::new();
    tables.insert("schemata".to_string(), schemata_table());
    tables.insert("tables".to_string(), tables_table(db_state));
    tables.insert("columns".to_string(), columns_table(db_state));
    tables.insert("statistics".to_string(), statistics_table(db_state));
    tables.insert(
        "key_column_usage".to_string(),
        key_column_usage_table(db_state),
    );
    tables.insert("indexes".to_string(), indexes_table(db_state));
    tables.insert("session_status".to_string(), session_status_table());
    tables.insert("global_status".to_string(), session_status_table());
    tables.insert("kv_strings".to_string(), kv_strings_table(db_state));
    tables.insert("kv_hash".to_string(), kv_hash_table(db_state));
    tables.insert("kv_list".to_string(), kv_list_table(db_state));
    tables.insert("kv_set".to_string(), kv_set_table(db_state));
    tables.insert("kv_zset".to_string(), kv_zset_table(db_state));
    tables.insert("kv_stream".to_string(), kv_stream_table(db_state));
    tables
}

pub fn session_status_table() -> Table {
    let cols = vec![
        column("VARIABLE_NAME", DataType::Text),
        column("VARIABLE_VALUE", DataType::Text),
    ];
    let mut table = Table::new("session_status".to_string(), cols, None, vec![]);
    table.data.rows.push(Row {
        id: "uptime".to_string(),
        values: vec![
            Value::Text("Uptime".to_string()),
            Value::Text("0".to_string()),
        ],
    });
    table
}

pub fn global_status_table() -> Table {
    session_status_table()
}

fn column(name: &str, data_type: DataType) -> Column {
    Column {
        name: name.to_string(),
        data_type,
        is_auto_increment: false,
        is_not_null: false,
        default_value: None,
    }
}

fn schemata_table() -> Table {
    let cols = vec![
        column("catalog_name", DataType::Text),
        column("schema_name", DataType::Text),
        column("default_character_set_name", DataType::Text),
    ];
    let mut table = Table::new("schemata".to_string(), cols, None, vec![]);
    table.data.rows.push(Row {
        id: "def".to_string(),
        values: vec![
            crate::storage::Value::Text("def".to_string()),
            crate::storage::Value::Text("default".to_string()),
            crate::storage::Value::Text("utf8".to_string()),
        ],
    });
    table
}

fn tables_table(db_state: &DatabaseState) -> Table {
    let cols = vec![
        column("table_schema", DataType::Text),
        column("table_name", DataType::Text),
        column("table_type", DataType::Text),
        column("row_count", DataType::Int),
    ];
    let mut table = Table::new("tables".to_string(), cols, None, vec![]);
    for (name, t) in &db_state.tables {
        table.data.rows.push(Row {
            id: name.clone(),
            values: vec![
                crate::storage::Value::Text("default".to_string()),
                crate::storage::Value::Text(name.clone()),
                crate::storage::Value::Text("BASE TABLE".to_string()),
                crate::storage::Value::Int(t.data.rows.len() as i64),
            ],
        });
    }
    for sys_view in &[
        "tables",
        "columns",
        "indexes",
        "schemata",
        "statistics",
        "key_column_usage",
        "kv_strings",
        "kv_hash",
        "kv_list",
        "kv_set",
        "kv_zset",
        "kv_stream",
    ] {
        table.data.rows.push(Row {
            id: sys_view.to_string(),
            values: vec![
                crate::storage::Value::Text("information_schema".to_string()),
                crate::storage::Value::Text(sys_view.to_string()),
                crate::storage::Value::Text("SYSTEM VIEW".to_string()),
                crate::storage::Value::Int(0),
            ],
        });
    }
    table
}

fn columns_table(db_state: &DatabaseState) -> Table {
    let cols = vec![
        column("table_schema", DataType::Text),
        column("table_name", DataType::Text),
        column("column_name", DataType::Text),
        column("data_type", DataType::Text),
        column("ordinal_position", DataType::Int),
        column("is_auto_increment", DataType::Bool),
    ];
    let mut table = Table::new("columns".to_string(), cols, None, vec![]);
    for (t_name, t) in &db_state.tables {
        for (i, col) in t.schema.columns.iter().enumerate() {
            table.data.rows.push(Row {
                id: format!("{}_{}", t_name, col.name),
                values: vec![
                    crate::storage::Value::Text("default".to_string()),
                    crate::storage::Value::Text(t_name.clone()),
                    crate::storage::Value::Text(col.name.clone()),
                    crate::storage::Value::Text(format!("{:?}", col.data_type).to_uppercase()),
                    crate::storage::Value::Int((i + 1) as i64),
                    crate::storage::Value::Bool(col.is_auto_increment),
                ],
            });
        }
    }
    table
}

fn statistics_table(db_state: &DatabaseState) -> Table {
    let cols = vec![
        column("table_schema", DataType::Text),
        column("table_name", DataType::Text),
        column("non_unique", DataType::Int),
        column("index_name", DataType::Text),
        column("seq_in_index", DataType::Int),
        column("column_name", DataType::Text),
        column("index_type", DataType::Text),
        column("cardinality", DataType::Int),
        column("total_rows", DataType::Int),
    ];
    let mut table = Table::new("statistics".to_string(), cols, None, vec![]);
    for (t_name, t) in &db_state.tables {
        for (idx_name, index) in &t.indexes.secondary {
            let non_unique = if index.is_unique() { 0 } else { 1 };
            let idx_type = match index {
                crate::storage::TableIndex::BTree { .. } => "BTREE",
                crate::storage::TableIndex::Hash { .. } => "HASH",
            };
            let cardinality = index.key_count();
            let total_rows = index.total_rows();
            for (i, expr) in index.expressions().iter().enumerate() {
                let col_name: String = match expr {
                    crate::squeal::ir::Expression::Column(c) => c.clone(),
                    _ => format!("expr_{}", i),
                };
                table.data.rows.push(Row {
                    id: format!("{}_{}_{}", t_name, idx_name, i),
                    values: vec![
                        crate::storage::Value::Text("default".to_string()),
                        crate::storage::Value::Text(t_name.clone()),
                        crate::storage::Value::Int(non_unique as i64),
                        crate::storage::Value::Text(idx_name.clone()),
                        crate::storage::Value::Int((i + 1) as i64),
                        crate::storage::Value::Text(col_name),
                        crate::storage::Value::Text(idx_type.to_string()),
                        crate::storage::Value::Int(cardinality as i64),
                        crate::storage::Value::Int(total_rows as i64),
                    ],
                });
            }
        }
    }
    table
}

fn key_column_usage_table(db_state: &DatabaseState) -> Table {
    let cols = vec![
        column("constraint_schema", DataType::Text),
        column("constraint_name", DataType::Text),
        column("table_schema", DataType::Text),
        column("table_name", DataType::Text),
        column("column_name", DataType::Text),
        column("referenced_table_schema", DataType::Text),
        column("referenced_table_name", DataType::Text),
        column("referenced_column_name", DataType::Text),
    ];
    let mut table = Table::new("key_column_usage".to_string(), cols, None, vec![]);
    for (t_name, t) in &db_state.tables {
        if let Some(ref pk_cols) = t.schema.primary_key {
            for col_name in pk_cols {
                let col_name: String = col_name.clone();
                table.data.rows.push(Row {
                    id: format!("{}_pk_{}", t_name, col_name),
                    values: vec![
                        crate::storage::Value::Text("default".to_string()),
                        crate::storage::Value::Text("PRIMARY".to_string()),
                        crate::storage::Value::Text("default".to_string()),
                        crate::storage::Value::Text(t_name.clone()),
                        crate::storage::Value::Text(col_name),
                        crate::storage::Value::Null,
                        crate::storage::Value::Null,
                        crate::storage::Value::Null,
                    ],
                });
            }
        }
        for fk in &t.schema.foreign_keys {
            let constraint_name = format!("fk_{}_{}", t_name, fk.ref_table);
            for (i, col_name) in fk.columns.iter().enumerate() {
                table.data.rows.push(Row {
                    id: format!("{}_{}_{}", t_name, constraint_name, i),
                    values: vec![
                        crate::storage::Value::Text("default".to_string()),
                        crate::storage::Value::Text(constraint_name.clone()),
                        crate::storage::Value::Text("default".to_string()),
                        crate::storage::Value::Text(t_name.clone()),
                        crate::storage::Value::Text(col_name.clone()),
                        crate::storage::Value::Text("default".to_string()),
                        crate::storage::Value::Text(fk.ref_table.clone()),
                        crate::storage::Value::Text(fk.ref_columns[i].clone()),
                    ],
                });
            }
        }
    }
    table
}

fn indexes_table(db_state: &DatabaseState) -> Table {
    let cols = vec![
        column("table_name", DataType::Text),
        column("index_name", DataType::Text),
        column("is_unique", DataType::Bool),
        column("index_type", DataType::Text),
    ];
    let mut table = Table::new("indexes".to_string(), cols, None, vec![]);
    for (t_name, t) in &db_state.tables {
        for (idx_name, index) in &t.indexes.secondary {
            let (is_unique, idx_type) = match index {
                crate::storage::TableIndex::BTree { unique, .. } => (*unique, "BTREE"),
                crate::storage::TableIndex::Hash { unique, .. } => (*unique, "HASH"),
            };
            table.data.rows.push(Row {
                id: format!("{}_{}", t_name, idx_name),
                values: vec![
                    crate::storage::Value::Text(t_name.clone()),
                    crate::storage::Value::Text(idx_name.clone()),
                    crate::storage::Value::Bool(is_unique),
                    crate::storage::Value::Text(idx_type.to_string()),
                ],
            });
        }
    }
    table
}

fn kv_strings_table(db_state: &DatabaseState) -> Table {
    let cols = vec![
        column("key", DataType::Text),
        column("value", DataType::Text),
        column("expiry", DataType::Int),
    ];
    let mut table = Table::new("kv_strings".to_string(), cols, None, vec![]);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    for (key, value) in &db_state.kv {
        let expiry = db_state.kv_expiry.get(key).map(|&e| e as i64).unwrap_or(-1);
        let expiry_display = if expiry > 0 && (expiry as u64) < now {
            -2
        } else {
            expiry
        };
        table.data.rows.push(Row {
            id: key.clone(),
            values: vec![
                crate::storage::Value::Text(key.clone()),
                crate::storage::Value::Text(format!("{:?}", value)),
                crate::storage::Value::Int(expiry_display),
            ],
        });
    }
    table
}

fn kv_hash_table(db_state: &DatabaseState) -> Table {
    let cols = vec![
        column("key", DataType::Text),
        column("field", DataType::Text),
        column("value", DataType::Text),
    ];
    let mut table = Table::new("kv_hash".to_string(), cols, None, vec![]);
    for (key, hash) in &db_state.kv_hash {
        for (field, value) in hash {
            table.data.rows.push(Row {
                id: format!("{}_{}", key, field),
                values: vec![
                    crate::storage::Value::Text(key.clone()),
                    crate::storage::Value::Text(field.clone()),
                    crate::storage::Value::Text(format!("{:?}", value)),
                ],
            });
        }
    }
    table
}

fn kv_list_table(db_state: &DatabaseState) -> Table {
    let cols = vec![
        column("key", DataType::Text),
        column("index", DataType::Int),
        column("value", DataType::Text),
    ];
    let mut table = Table::new("kv_list".to_string(), cols, None, vec![]);
    for (key, list) in &db_state.kv_list {
        for (i, value) in list.iter().enumerate() {
            table.data.rows.push(Row {
                id: format!("{}_{}", key, i),
                values: vec![
                    crate::storage::Value::Text(key.clone()),
                    crate::storage::Value::Int(i as i64),
                    crate::storage::Value::Text(format!("{:?}", value)),
                ],
            });
        }
    }
    table
}

fn kv_set_table(db_state: &DatabaseState) -> Table {
    let cols = vec![
        column("key", DataType::Text),
        column("member", DataType::Text),
    ];
    let mut table = Table::new("kv_set".to_string(), cols, None, vec![]);
    for (key, set) in &db_state.kv_set {
        for member in set {
            table.data.rows.push(Row {
                id: format!("{}_{}", key, member),
                values: vec![
                    crate::storage::Value::Text(key.clone()),
                    crate::storage::Value::Text(member.clone()),
                ],
            });
        }
    }
    table
}

fn kv_zset_table(db_state: &DatabaseState) -> Table {
    let cols = vec![
        column("key", DataType::Text),
        column("score", DataType::Float),
        column("member", DataType::Text),
    ];
    let mut table = Table::new("kv_zset".to_string(), cols, None, vec![]);
    for (key, zset) in &db_state.kv_zset {
        for (score, member) in zset {
            table.data.rows.push(Row {
                id: format!("{}_{}", key, member),
                values: vec![
                    crate::storage::Value::Text(key.clone()),
                    crate::storage::Value::Float(*score),
                    crate::storage::Value::Text(member.clone()),
                ],
            });
        }
    }
    table
}

fn kv_stream_table(db_state: &DatabaseState) -> Table {
    let cols = vec![
        column("key", DataType::Text),
        column("id", DataType::Int),
        column("field", DataType::Text),
        column("value", DataType::Text),
    ];
    let mut table = Table::new("kv_stream".to_string(), cols, None, vec![]);
    for (key, stream) in &db_state.kv_stream {
        for (id, fields) in stream {
            for (field, value) in fields {
                table.data.rows.push(Row {
                    id: format!("{}_{}_{}", key, id, field),
                    values: vec![
                        crate::storage::Value::Text(key.clone()),
                        crate::storage::Value::Int(*id as i64),
                        crate::storage::Value::Text(field.clone()),
                        crate::storage::Value::Text(format!("{:?}", value)),
                    ],
                });
            }
        }
    }
    table
}
