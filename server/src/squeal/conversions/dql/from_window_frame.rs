use crate::sql::ast;
use crate::squeal::expr::*;

impl From<ast::WindowFrame> for WindowFrame {
    fn from(f: ast::WindowFrame) -> Self {
        WindowFrame {
            units: match f.units {
                ast::FrameUnits::Rows => FrameUnits::Rows,
                ast::FrameUnits::Range => FrameUnits::Range,
            },
            start: Box::new((*f.start).into()),
            end: Box::new((*f.end).into()),
        }
    }
}
