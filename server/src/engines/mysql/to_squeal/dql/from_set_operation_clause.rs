use crate::engines::mysql::ast;
use crate::squeal::ir::stmt::*;

impl From<ast::SetOperationClause> for SetOperationClause {
    fn from(s: ast::SetOperationClause) -> Self {
        SetOperationClause {
            operator: match s.operator {
                ast::SetOperator::Union => SetOperator::Union,
                ast::SetOperator::UnionAll => SetOperator::UnionAll,
                ast::SetOperator::Intersect => SetOperator::Intersect,
                ast::SetOperator::Except => SetOperator::Except,
            },
            select: Box::new((*s.select).into()),
        }
    }
}
