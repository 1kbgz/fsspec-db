use std::ops::ControlFlow;

use sqlparser::dialect::{
    Dialect as SqlDialect, GenericDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect,
};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::types::Dialect;
use crate::{DbError, Result};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SelectOptions {
    pub columns: Vec<String>,
    pub limit: Option<u64>,
    /// A pre-validated, canonicalized boolean predicate (see [`validate_predicate`]).
    pub where_clause: Option<String>,
}

fn sqlparser_dialect(dialect: &Dialect) -> Box<dyn SqlDialect> {
    match dialect {
        Dialect::Sqlite => Box::new(SQLiteDialect {}),
        Dialect::Postgres => Box::new(PostgreSqlDialect {}),
        Dialect::MySql => Box::new(MySqlDialect {}),
        Dialect::Generic => Box::new(GenericDialect {}),
    }
}

/// Parse a user-supplied `?where=` predicate, reject anything that is not a
/// single boolean expression, and return its canonical re-serialized form.
///
/// Re-serializing from the AST is what makes this injection-safe: a payload
/// such as `1=1; DROP TABLE x` parses an expression (`1=1`) and then leaves
/// trailing tokens, which we reject; the emitted SQL only ever comes from the
/// parsed expression, never the raw input.
pub fn validate_predicate(dialect: &Dialect, predicate: &str) -> Result<String> {
    let predicate = predicate.trim();
    if predicate.is_empty() {
        return Err(DbError::InvalidArgument(
            "where clause must not be empty".to_string(),
        ));
    }
    let sql_dialect = sqlparser_dialect(dialect);
    let mut parser = Parser::new(sql_dialect.as_ref())
        .try_with_sql(predicate)
        .map_err(|err| DbError::InvalidArgument(format!("invalid where clause: {err}")))?;
    let expr = parser
        .parse_expr()
        .map_err(|err| DbError::InvalidArgument(format!("invalid where clause: {err}")))?;
    if parser.peek_token().token != Token::EOF {
        return Err(DbError::InvalidArgument(
            "where clause must be a single boolean expression".to_string(),
        ));
    }
    Ok(expr.to_string())
}

/// Best-effort extraction of the relations a view depends on, by parsing its
/// definition. Returns an empty list (never an error) when the definition
/// cannot be parsed, so listing a view never fails on a parser limitation.
pub fn view_dependencies(dialect: &Dialect, definition: &str) -> Vec<String> {
    let definition = definition.trim();
    if definition.is_empty() {
        return Vec::new();
    }
    let sql_dialect = sqlparser_dialect(dialect);
    let Ok(statements) = Parser::parse_sql(sql_dialect.as_ref(), definition) else {
        return Vec::new();
    };
    let mut tables: Vec<String> = Vec::new();
    let _ = sqlparser::ast::visit_relations(&statements, |name| {
        let rendered = name
            .0
            .iter()
            .map(|part| part.to_string().trim_matches(['"', '`', '\'']).to_string())
            .collect::<Vec<_>>()
            .join(".");
        if !rendered.is_empty() && !tables.contains(&rendered) {
            tables.push(rendered);
        }
        ControlFlow::<()>::Continue(())
    });
    tables
}

pub fn quote_identifier(dialect: &Dialect, identifier: &str) -> Result<String> {
    if identifier.is_empty() || identifier.contains('\0') {
        return Err(DbError::InvalidArgument(format!(
            "invalid SQL identifier: {identifier:?}"
        )));
    }
    match dialect {
        Dialect::MySql => Ok(format!("`{}`", identifier.replace('`', "``"))),
        _ => Ok(format!("\"{}\"", identifier.replace('"', "\"\""))),
    }
}

pub fn select_sql(
    dialect: &Dialect,
    schema: &str,
    relation: &str,
    options: &SelectOptions,
) -> Result<String> {
    let columns = if options.columns.is_empty() {
        "*".to_string()
    } else {
        options
            .columns
            .iter()
            .map(|column| quote_identifier(dialect, column))
            .collect::<Result<Vec<_>>>()?
            .join(", ")
    };
    let relation = qualified_name(dialect, schema, relation)?;
    let mut sql = format!("SELECT {columns} FROM {relation}");
    if let Some(predicate) = &options.where_clause {
        sql.push_str(&format!(" WHERE {predicate}"));
    }
    if let Some(limit) = options.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    Ok(sql)
}

pub fn insert_sql(
    dialect: &Dialect,
    schema: &str,
    relation: &str,
    columns: &[String],
    rows: usize,
) -> Result<String> {
    if columns.is_empty() {
        return Err(DbError::InvalidArgument(
            "INSERT requires at least one column".to_string(),
        ));
    }
    if rows == 0 {
        return Err(DbError::InvalidArgument(
            "INSERT requires at least one row".to_string(),
        ));
    }

    let relation = qualified_name(dialect, schema, relation)?;
    let column_count = columns.len();
    let columns = columns
        .iter()
        .map(|column| quote_identifier(dialect, column))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let mut index = 1usize;
    let values = (0..rows)
        .map(|_| {
            let row = (0..column_count)
                .map(|_| {
                    let placeholder = placeholder(dialect, index);
                    index += 1;
                    placeholder
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("({row})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "INSERT INTO {relation} ({columns}) VALUES {values}"
    ))
}

fn qualified_name(dialect: &Dialect, schema: &str, relation: &str) -> Result<String> {
    Ok(format!(
        "{}.{}",
        quote_identifier(dialect, schema)?,
        quote_identifier(dialect, relation)?
    ))
}

fn placeholder(dialect: &Dialect, index: usize) -> String {
    match dialect {
        Dialect::Postgres => format!("${index}"),
        _ => "?".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers_by_dialect() {
        assert_eq!(
            quote_identifier(&Dialect::Postgres, "weird\"name").unwrap(),
            "\"weird\"\"name\""
        );
        assert_eq!(
            quote_identifier(&Dialect::MySql, "weird`name").unwrap(),
            "`weird``name`"
        );
    }

    #[test]
    fn builds_select() {
        let sql = select_sql(
            &Dialect::Sqlite,
            "main",
            "users",
            &SelectOptions {
                columns: vec!["id".to_string(), "name".to_string()],
                limit: Some(5),
                where_clause: None,
            },
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"main\".\"users\" LIMIT 5"
        );
    }

    #[test]
    fn builds_select_with_where() {
        let sql = select_sql(
            &Dialect::Sqlite,
            "main",
            "users",
            &SelectOptions {
                columns: Vec::new(),
                limit: Some(10),
                where_clause: Some(validate_predicate(&Dialect::Sqlite, "score > 1").unwrap()),
            },
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM \"main\".\"users\" WHERE score > 1 LIMIT 10"
        );
    }

    #[test]
    fn validates_and_canonicalizes_predicate() {
        assert_eq!(
            validate_predicate(&Dialect::Sqlite, "id = 1 AND name IS NOT NULL").unwrap(),
            "id = 1 AND name IS NOT NULL"
        );
    }

    #[test]
    fn rejects_injection_in_predicate() {
        // Trailing statement after a valid expression must be rejected.
        assert!(matches!(
            validate_predicate(&Dialect::Sqlite, "1=1); DROP TABLE users;--"),
            Err(DbError::InvalidArgument(_))
        ));
        // A bare statement is not a single expression.
        assert!(matches!(
            validate_predicate(&Dialect::Sqlite, "DROP TABLE users"),
            Err(DbError::InvalidArgument(_))
        ));
        assert!(matches!(
            validate_predicate(&Dialect::Sqlite, "   "),
            Err(DbError::InvalidArgument(_))
        ));
    }

    #[test]
    fn extracts_view_dependencies() {
        let deps = view_dependencies(
            &Dialect::Sqlite,
            "CREATE VIEW active AS SELECT u.id FROM users u JOIN accounts a ON a.id = u.id",
        );
        assert!(deps.contains(&"users".to_string()));
        assert!(deps.contains(&"accounts".to_string()));

        // Postgres-style bare SELECT body (as returned by pg_get_viewdef).
        let pg = view_dependencies(&Dialect::Postgres, "SELECT id, name FROM public.people");
        assert_eq!(pg, vec!["public.people".to_string()]);

        // Unparseable definitions degrade to empty rather than erroring.
        assert!(view_dependencies(&Dialect::Sqlite, "<<not sql>>").is_empty());
    }

    #[test]
    fn builds_postgres_insert() {
        let sql = insert_sql(
            &Dialect::Postgres,
            "public",
            "users",
            &["id".to_string(), "name".to_string()],
            2,
        )
        .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"public\".\"users\" (\"id\", \"name\") VALUES ($1, $2), ($3, $4)"
        );
    }

    #[test]
    fn builds_mysql_insert() {
        let sql = insert_sql(
            &Dialect::MySql,
            "app",
            "users",
            &["id".to_string(), "name".to_string()],
            2,
        )
        .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO `app`.`users` (`id`, `name`) VALUES (?, ?), (?, ?)"
        );
    }
}
