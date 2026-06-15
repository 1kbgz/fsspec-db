#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaInfo {
    pub name: String,
    pub catalog: Option<String>,
    pub comment: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelationKind {
    Table,
    View,
}

impl RelationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            RelationKind::Table => "table",
            RelationKind::View => "view",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationInfo {
    pub name: String,
    pub kind: RelationKind,
    pub row_count: Option<u64>,
    pub size_bytes: Option<u64>,
    pub comment: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub ordinal: u32,
    pub primary_key: bool,
    pub comment: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub method: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstraintKind {
    PrimaryKey,
    ForeignKey,
    Unique,
    Check,
}

impl ConstraintKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConstraintKind::PrimaryKey => "pk",
            ConstraintKind::ForeignKey => "fk",
            ConstraintKind::Unique => "unique",
            ConstraintKind::Check => "check",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConstraintInfo {
    pub name: String,
    pub kind: ConstraintKind,
    pub columns: Vec<String>,
    pub references: Option<String>,
    pub expr: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Dialect {
    Generic,
    Sqlite,
    Postgres,
    MySql,
}

impl Dialect {
    pub fn as_str(&self) -> &'static str {
        match self {
            Dialect::Generic => "generic",
            Dialect::Sqlite => "sqlite",
            Dialect::Postgres => "postgres",
            Dialect::MySql => "mysql",
        }
    }
}
