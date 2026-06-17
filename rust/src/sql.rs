use crate::types::Dialect;
use crate::{DbError, Result};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SelectOptions {
    pub columns: Vec<String>,
    pub limit: Option<u64>,
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
            },
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"main\".\"users\" LIMIT 5"
        );
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
