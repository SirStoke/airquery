use crate::dataflow::Properties;
use sqlparser::ast::{Select, SetExpr, Statement};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CompilerError {
    #[error("unsupported feature: '{0}'")]
    Unsupported(String),
}

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

fn compile_select<'a>(select: &Select) -> Result<Properties<'a>, CompilerError> {
    check_unsupported(select)?;

    todo!()
}

fn check_unsupported(select: &Select) -> Result<(), CompilerError> {
    check_empty(&select.lateral_views, "LATERAL VIEW")?;
    check_empty(&select.group_by, "GROUP BY")?;
    check_empty(&select.cluster_by, "CLUSTER BY")?;
    check_empty(&select.distribute_by, "DISTRIBUTE BY")?;
    check_empty(&select.sort_by, "ORDER BY")?;

    check_none(&select.top, "TOP")?;
    check_none(&select.selection, "WHERE")?;
    check_none(&select.having, "HAVING")?;
    check_none(&select.qualify, "QUALIFY")?;

    let has_joins = select.from.iter().any(|from| !from.joins.is_empty());

    check_bool(has_joins, "JOIN")?;

    Ok(())
}

#[inline(always)]
fn check_empty<T>(vec: &Vec<T>, msg: &str) -> Result<(), CompilerError> {
    check_bool(!vec.is_empty(), msg)
}

#[inline(always)]
fn check_none<T>(option: &Option<T>, msg: &str) -> Result<(), CompilerError> {
    check_bool(option.is_some(), msg)
}

#[inline(always)]
fn check_bool(is_unsupported: bool, msg: &str) -> Result<(), CompilerError> {
    if is_unsupported {
        Err(CompilerError::Unsupported(msg.to_string()))
    } else {
        Ok(())
    }
}
