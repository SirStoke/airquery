use crate::dataflow::Properties;
use sqlparser::ast::{Select, SetExpr, Statement};
use thiserror::Error;

/// Macro that accepts a boolean and a str, returning an error if the boolean is true
macro_rules! check_bool {
    ($is_unsupported:expr, $msg:expr) => {
        if $is_unsupported {
            Err(CompilerError::Unsupported($msg.to_string()))?;
        }
    };
}

/// Expects the first argument to be a vector, errors out if is not empty
macro_rules! check_empty {
    ($vec:expr, $msg:expr) => {
        check_bool!(!$vec.is_empty(), $msg)
    };
}

/// Expects the first argument to be an option, errors out if it's "some"
macro_rules! check_none {
    ($option:expr, $msg:expr) => {
        check_bool!($option.is_some(), $msg)
    };
}

#[derive(Error, Debug)]
pub enum CompilerError {
    #[error("unsupported feature: '{0}'")]
    Unsupported(String),
}

/// Compiles a list of SQL statements into physical properties that can be then picked up by a query
/// planner
pub fn compile<'a>(statements: &Vec<Statement>) -> Result<Properties<'a>, CompilerError> {
    if statements.len() != 1 {
        Err(CompilerError::Unsupported(
            "multiple statements".to_string(),
        ))?;
    }

    let statement = &statements[0];

    let query = match statement {
        Statement::Query(query) => query,
        _ => Err(CompilerError::Unsupported(
            "only SELECTs are supported".to_string(),
        ))?,
    };

    let select = match &query.body {
        SetExpr::Select(select) => select,
        _ => Err(CompilerError::Unsupported(
            "only SELECTs are supported".to_string(),
        ))?,
    };

    compile_select(select)
}

/// Compiles a select statement into physical properties
fn compile_select<'a>(select: &Select) -> Result<Properties<'a>, CompilerError> {
    check_unsupported(select)?;

    todo!()
}

fn check_unsupported(select: &Select) -> Result<(), CompilerError> {
    check_empty!(select.lateral_views, "LATERAL VIEW");
    check_empty!(select.group_by, "GROUP BY");
    check_empty!(select.cluster_by, "CLUSTER BY");
    check_empty!(select.distribute_by, "DISTRIBUTE BY");
    check_empty!(select.sort_by, "ORDER BY");

    check_none!(select.top, "TOP");
    check_none!(select.selection, "WHERE");
    check_none!(select.having, "HAVING");
    check_none!(select.qualify, "QUALIFY");

    let has_joins = select.from.iter().any(|from| !from.joins.is_empty());

    check_bool!(has_joins, "JOIN");

    Ok(())
}
