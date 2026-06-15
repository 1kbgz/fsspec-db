use std::fmt;

use crate::{DbError, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataFormat {
    Parquet,
    Arrow,
    Csv,
    Jsonl,
    Sql,
}

impl DataFormat {
    pub fn extension(&self) -> &'static str {
        match self {
            DataFormat::Parquet => "parquet",
            DataFormat::Arrow => "arrow",
            DataFormat::Csv => "csv",
            DataFormat::Jsonl => "jsonl",
            DataFormat::Sql => "sql",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "parquet" => Some(DataFormat::Parquet),
            "arrow" => Some(DataFormat::Arrow),
            "csv" => Some(DataFormat::Csv),
            "jsonl" => Some(DataFormat::Jsonl),
            "sql" => Some(DataFormat::Sql),
            _ => None,
        }
    }
}

impl fmt::Display for DataFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.extension())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DbFacet {
    Columns,
    Indexes,
    Constraints,
    DependsOn,
}

impl DbFacet {
    pub fn as_str(&self) -> &'static str {
        match self {
            DbFacet::Columns => "columns",
            DbFacet::Indexes => "indexes",
            DbFacet::Constraints => "constraints",
            DbFacet::DependsOn => "depends_on",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "columns" => Some(DbFacet::Columns),
            "indexes" => Some(DbFacet::Indexes),
            "constraints" => Some(DbFacet::Constraints),
            "depends_on" => Some(DbFacet::DependsOn),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DbPathKind {
    Root,
    Schema,
    Relation,
    Facet {
        facet: DbFacet,
        item: Option<String>,
    },
    RelationData {
        format: DataFormat,
    },
    ViewDefinition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DbPath {
    pub schema: Option<String>,
    pub relation: Option<String>,
    pub kind: DbPathKind,
    pub query: Vec<(String, String)>,
}

impl DbPath {
    pub fn root() -> Self {
        Self {
            schema: None,
            relation: None,
            kind: DbPathKind::Root,
            query: Vec::new(),
        }
    }

    pub fn parse(path: &str) -> Result<Self> {
        let stripped = strip_protocol(path);
        let (path_part, query) = split_query(&stripped);
        let clean = normalize_path(path_part);
        if clean == "/" {
            return Ok(Self {
                query,
                ..Self::root()
            });
        }

        let parts: Vec<&str> = clean.trim_start_matches('/').split('/').collect();
        match parts.as_slice() {
            [schema] => Ok(Self {
                schema: Some((*schema).to_string()),
                relation: None,
                kind: DbPathKind::Schema,
                query,
            }),
            [schema, relation_part] => {
                if let Some((relation, format)) = split_relation_format(relation_part) {
                    return Ok(Self {
                        schema: Some((*schema).to_string()),
                        relation: Some(relation),
                        kind: DbPathKind::RelationData { format },
                        query,
                    });
                }
                Ok(Self {
                    schema: Some((*schema).to_string()),
                    relation: Some((*relation_part).to_string()),
                    kind: DbPathKind::Relation,
                    query,
                })
            }
            [schema, relation, "definition.sql"] => Ok(Self {
                schema: Some((*schema).to_string()),
                relation: Some((*relation).to_string()),
                kind: DbPathKind::ViewDefinition,
                query,
            }),
            [schema, relation, facet] => {
                let facet = DbFacet::parse(facet).ok_or_else(|| {
                    DbError::InvalidArgument(format!("unknown database path facet: {facet}"))
                })?;
                Ok(Self {
                    schema: Some((*schema).to_string()),
                    relation: Some((*relation).to_string()),
                    kind: DbPathKind::Facet { facet, item: None },
                    query,
                })
            }
            [schema, relation, facet, item] => {
                let facet = DbFacet::parse(facet).ok_or_else(|| {
                    DbError::InvalidArgument(format!("unknown database path facet: {facet}"))
                })?;
                Ok(Self {
                    schema: Some((*schema).to_string()),
                    relation: Some((*relation).to_string()),
                    kind: DbPathKind::Facet {
                        facet,
                        item: Some((*item).to_string()),
                    },
                    query,
                })
            }
            _ => Err(DbError::InvalidArgument(format!(
                "unsupported database path: {path}"
            ))),
        }
    }

    pub fn to_path(&self) -> String {
        let mut path = match (&self.schema, &self.relation, &self.kind) {
            (_, _, DbPathKind::Root) => "/".to_string(),
            (Some(schema), _, DbPathKind::Schema) => format!("/{schema}"),
            (Some(schema), Some(relation), DbPathKind::Relation) => {
                format!("/{schema}/{relation}")
            }
            (Some(schema), Some(relation), DbPathKind::Facet { facet, item }) => {
                let base = format!("/{schema}/{relation}/{}", facet.as_str());
                match item {
                    Some(item) => format!("{base}/{item}"),
                    None => base,
                }
            }
            (Some(schema), Some(relation), DbPathKind::RelationData { format }) => {
                format!("/{schema}/{relation}.{}", format.extension())
            }
            (Some(schema), Some(relation), DbPathKind::ViewDefinition) => {
                format!("/{schema}/{relation}/definition.sql")
            }
            _ => "/".to_string(),
        };

        if !self.query.is_empty() {
            let query = self
                .query
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&");
            path.push('?');
            path.push_str(&query);
        }
        path
    }

    pub fn schema_path(schema: &str) -> String {
        format!("/{schema}")
    }

    pub fn relation_path(schema: &str, relation: &str) -> String {
        format!("/{schema}/{relation}")
    }

    pub fn facet_path(schema: &str, relation: &str, facet: DbFacet) -> String {
        format!("/{schema}/{relation}/{}", facet.as_str())
    }

    pub fn facet_item_path(schema: &str, relation: &str, facet: DbFacet, item: &str) -> String {
        format!("/{schema}/{relation}/{}/{item}", facet.as_str())
    }

    pub fn relation_data_path(schema: &str, relation: &str, format: DataFormat) -> String {
        format!("/{schema}/{relation}.{}", format.extension())
    }

    pub fn view_definition_path(schema: &str, relation: &str) -> String {
        format!("/{schema}/{relation}/definition.sql")
    }
}

fn strip_protocol(path: &str) -> String {
    match path.split_once("://") {
        Some((_, rest)) => rest.to_string(),
        None => path.to_string(),
    }
}

fn split_query(path: &str) -> (&str, Vec<(String, String)>) {
    let Some((path_part, query_part)) = path.split_once('?') else {
        return (path, Vec::new());
    };
    let query = query_part
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| match part.split_once('=') {
            Some((key, value)) => (key.to_string(), value.to_string()),
            None => (part.to_string(), String::new()),
        })
        .collect();
    (path_part, query)
}

fn normalize_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return "/".to_string();
    }
    let collapsed = trimmed
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    if collapsed.is_empty() {
        "/".to_string()
    } else {
        format!("/{collapsed}")
    }
}

fn split_relation_format(value: &str) -> Option<(String, DataFormat)> {
    let (relation, ext) = value.rsplit_once('.')?;
    Some((relation.to_string(), DataFormat::parse(ext)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_root() {
        assert_eq!(DbPath::parse("/").unwrap(), DbPath::root());
        assert_eq!(DbPath::parse("db://").unwrap(), DbPath::root());
    }

    #[test]
    fn parses_relation_data_with_query() {
        let path = DbPath::parse("/main/users.parquet?limit=10&columns=id,name").unwrap();
        assert_eq!(path.schema.as_deref(), Some("main"));
        assert_eq!(path.relation.as_deref(), Some("users"));
        assert_eq!(
            path.kind,
            DbPathKind::RelationData {
                format: DataFormat::Parquet
            }
        );
        assert_eq!(
            path.to_path(),
            "/main/users.parquet?limit=10&columns=id,name"
        );
    }

    #[test]
    fn parses_facet_item_roundtrip() {
        let path = DbPath::parse("db://main/users/columns/id").unwrap();
        assert_eq!(
            path,
            DbPath {
                schema: Some("main".to_string()),
                relation: Some("users".to_string()),
                kind: DbPathKind::Facet {
                    facet: DbFacet::Columns,
                    item: Some("id".to_string())
                },
                query: Vec::new()
            }
        );
        assert_eq!(path.to_path(), "/main/users/columns/id");
    }

    #[test]
    fn collapses_repeated_slashes() {
        let path = DbPath::parse("//main///users//columns/id").unwrap();
        assert_eq!(path.to_path(), "/main/users/columns/id");
    }
}
