use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::CreateIndexStmt> for CreateIndex {
    fn from(s: ast::CreateIndexStmt) -> Self {
        CreateIndex {
            name: s.name,
            table: s.table,
            expressions: s.expressions.into_iter().map(|e| e.into()).collect(),
            unique: s.unique,
            index_type: match s.index_type {
                ast::IndexType::BTree => IndexType::BTree,
                ast::IndexType::Hash => IndexType::Hash,
            },
            where_clause: s.where_clause.map(|w| w.into()),
        }
    }
}
