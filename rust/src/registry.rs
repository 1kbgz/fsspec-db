//! Factories that open databases from `db+sqlite://`, `db+postgres://`,
//! `db+postgresql://`, and `db+mysql://` URLs through an fsspec-rs
//! [`FilesystemRegistry`].

use std::sync::Arc;

use fsspec_rs::{
    FilesystemFactory, FilesystemRegistry, FsError, FsOptions, FsResult, SharedFileSystem,
};

use crate::{DatabaseFs, DbPoolOptions, MySqlDatabase, PostgresDatabase, SqliteDatabase};

const SQLITE_SUFFIXES: &[&str] = &[".db", ".sqlite", ".sqlite3"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Backend {
    Sqlite,
    Postgres,
    MySql,
}

/// Opens a [`DatabaseFs`] for one database backend.
///
/// The database comes from the URL as in the Python filesystems: the first path
/// segment ending in `.db`, `.sqlite`, or `.sqlite3` for SQLite, and `netloc/database`
/// for PostgreSQL and MySQL. Each database gets its own cached instance, and the
/// rest of the URL is the path inside it.
///
/// Options mirror the Python storage options. SQLite takes `database`, `path`, or
/// `url`. PostgreSQL and MySQL take `dsn`, `url`, or `host`, `port`, `database`
/// (`dbname`), `user` (`username`), and `password`, plus `min_connections`
/// (`min_pool_size`) and `max_connections` (`max_pool_size`). PostgreSQL also takes
/// `sslmode`; MySQL takes `ssl_mode` (`sslmode`) and `charset`. A `dsn` option comes
/// first, then the URL, then `url`, then the connection fields. Other options are
/// rejected.
#[derive(Clone, Copy, Debug)]
pub struct DatabaseFsFactory {
    backend: Backend,
}

impl DatabaseFsFactory {
    pub fn sqlite() -> Self {
        Self {
            backend: Backend::Sqlite,
        }
    }

    pub fn postgres() -> Self {
        Self {
            backend: Backend::Postgres,
        }
    }

    pub fn mysql() -> Self {
        Self {
            backend: Backend::MySql,
        }
    }
}

/// Registers `db+sqlite`, `db+postgres`, `db+postgresql`, and `db+mysql`.
pub fn register(registry: &mut FilesystemRegistry) {
    registry.register("db+sqlite", DatabaseFsFactory::sqlite());
    registry.register("db+postgres", DatabaseFsFactory::postgres());
    registry.register("db+postgresql", DatabaseFsFactory::postgres());
    registry.register("db+mysql", DatabaseFsFactory::mysql());
}

impl FilesystemFactory for DatabaseFsFactory {
    fn create(&self, path: &str, options: &FsOptions) -> FsResult<SharedFileSystem> {
        let location = self.instance_scope(path);
        let fs: SharedFileSystem = match self.backend {
            Backend::Sqlite => {
                let source = sqlite_source(&location, options)?;
                Arc::new(DatabaseFs::new(SqliteDatabase::connect(&source)?).with_location(location))
            }
            Backend::Postgres => {
                let (source, pool) = server_source("postgresql", &location, options)?;
                let db = PostgresDatabase::connect_with_pool_options(&source, pool)?;
                Arc::new(DatabaseFs::new(db).with_location(location))
            }
            Backend::MySql => {
                let (source, pool) = server_source("mysql", &location, options)?;
                let db = MySqlDatabase::connect_with_pool_options(&source, pool)?;
                Arc::new(DatabaseFs::new(db).with_location(location))
            }
        };
        Ok(fs)
    }

    fn instance_scope(&self, path: &str) -> String {
        match self.backend {
            Backend::Sqlite => sqlite_location(path),
            Backend::Postgres | Backend::MySql => server_location(path),
        }
    }
}

/// The URL part naming a SQLite database: through the first segment with a SQLite
/// suffix, the whole path when there is none, or `localhost` for a database given
/// only in options.
fn sqlite_location(path: &str) -> String {
    if path == "localhost" || path.starts_with("localhost/") {
        return "localhost".to_string();
    }
    let path = path.split('?').next().unwrap_or_default();
    let parts: Vec<&str> = path.split('/').collect();
    match parts.iter().position(|part| {
        let part = part.to_lowercase();
        SQLITE_SUFFIXES.iter().any(|suffix| part.ends_with(suffix))
    }) {
        Some(index) => parts[..=index].join("/"),
        None => path.to_string(),
    }
}

/// The URL part naming a server database, `netloc/database`. A query belongs to the
/// connection when no path inside the database follows.
fn server_location(path: &str) -> String {
    let (location, query) = match path.split_once('?') {
        Some((location, query)) => (location, Some(query)),
        None => (path, None),
    };
    let (netloc, tail) = location.split_once('/').unwrap_or((location, ""));
    let (database, inner) = tail.split_once('/').unwrap_or((tail, ""));
    let mut scope = netloc.to_string();
    if !database.is_empty() {
        scope = format!("{scope}/{database}");
    }
    match query {
        Some(query) if inner.trim_matches('/').is_empty() => format!("{scope}?{query}"),
        _ => scope,
    }
}

fn sqlite_source(location: &str, options: &FsOptions) -> FsResult<String> {
    let mut source = None;
    for (key, value) in options {
        match key.as_str() {
            "database" | "path" | "url" => {
                // `database` wins over its `path` and `url` aliases.
                if key == "database" || source.is_none() {
                    source = Some(value.clone());
                }
            }
            _ => return Err(unknown_option("db+sqlite", key)),
        }
    }
    match source {
        Some(source) => Ok(source),
        None if !location.is_empty() && location != "localhost" => Ok(location.to_string()),
        None => Err(FsError::InvalidArgument(
            "db+sqlite requires a database path or URL".to_string(),
        )),
    }
}

fn server_source(
    scheme: &str,
    location: &str,
    options: &FsOptions,
) -> FsResult<(String, DbPoolOptions)> {
    let protocol = format!("db+{scheme}");
    let get = |keys: &[&str]| keys.iter().find_map(|key| options.get(*key)).cloned();
    let mut known = vec![
        "dsn",
        "url",
        "host",
        "port",
        "database",
        "dbname",
        "user",
        "username",
        "password",
        "min_connections",
        "min_pool_size",
        "max_connections",
        "max_pool_size",
    ];
    let mut query = Vec::new();
    if scheme == "postgresql" {
        known.push("sslmode");
        if let Some(sslmode) = get(&["sslmode"]) {
            query.push(format!("sslmode={}", quote(&sslmode)));
        }
    } else {
        known.extend(["ssl_mode", "sslmode", "charset"]);
        if let Some(ssl_mode) = get(&["ssl_mode", "sslmode"]) {
            query.push(format!("ssl-mode={}", quote(&ssl_mode)));
        }
        if let Some(charset) = get(&["charset"]) {
            query.push(format!("charset={}", quote(&charset)));
        }
    }
    if let Some(key) = options.keys().find(|key| !known.contains(&key.as_str())) {
        return Err(unknown_option(&protocol, key));
    }

    let pool = DbPoolOptions::new(
        parse_count(&["min_connections", "min_pool_size"], options)?,
        parse_count(&["max_connections", "max_pool_size"], options)?,
    );
    let url_source = (!location.is_empty()).then(|| format!("{scheme}://{location}"));
    let field_source = get(&["host"]).map(|host| {
        let mut netloc = String::new();
        if let Some(user) = get(&["user", "username"]) {
            netloc.push_str(&quote(&user));
            if let Some(password) = get(&["password"]) {
                netloc.push(':');
                netloc.push_str(&quote(&password));
            }
            netloc.push('@');
        }
        netloc.push_str(&host);
        if let Some(port) = get(&["port"]) {
            netloc.push(':');
            netloc.push_str(&port);
        }
        let database = get(&["database", "dbname"])
            .map(|database| format!("/{}", quote(&database)))
            .unwrap_or_default();
        let query = if query.is_empty() {
            String::new()
        } else {
            format!("?{}", query.join("&"))
        };
        format!("{scheme}://{netloc}{database}{query}")
    });
    let source = get(&["dsn"])
        .or(url_source)
        .or_else(|| get(&["url"]))
        .or(field_source)
        .ok_or_else(|| {
            FsError::InvalidArgument(format!("{protocol} requires a DSN or host config"))
        })?;
    Ok((source, pool))
}

fn parse_count(keys: &[&str], options: &FsOptions) -> FsResult<Option<u32>> {
    keys.iter()
        .find_map(|key| options.get(*key).map(|value| (key, value)))
        .map(|(key, value)| {
            value.parse().map_err(|_| {
                FsError::InvalidArgument(format!(
                    "{key} must be a non-negative integer, got {value:?}"
                ))
            })
        })
        .transpose()
}

/// Percent-encodes everything except unreserved characters, like Python's
/// `urllib.parse.quote(value, safe="")`.
fn quote(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || b"_.-~".contains(&byte) {
                (byte as char).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}

fn unknown_option(protocol: &str, key: &str) -> FsError {
    FsError::InvalidArgument(format!("unknown {protocol} option: {key}"))
}

#[cfg(test)]
mod tests {
    use fsspec_rs::FileSystem;

    use super::*;

    fn options(pairs: &[(&str, &str)]) -> FsOptions {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn registry() -> FilesystemRegistry {
        let mut registry = FilesystemRegistry::empty();
        register(&mut registry);
        registry
    }

    #[test]
    fn sqlite_urls_resolve_to_one_filesystem_per_database() {
        let dir = std::env::temp_dir().join(format!("fsspec-db-registry-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let database = dir.join("state.db");
        let database = database.to_str().unwrap();
        let registry = registry();

        let (fs, path) = registry
            .url_to_fs(&format!("db+sqlite://{database}/main"), &FsOptions::new())
            .unwrap();
        assert_eq!(path, "/main");
        let (same, root) = registry
            .url_to_fs(&format!("db+sqlite://{database}"), &FsOptions::new())
            .unwrap();
        assert!(Arc::ptr_eq(&fs, &same));
        assert_eq!(root, "/");
        assert!(fs
            .ls(&path, true)
            .unwrap()
            .iter()
            .all(|entry| entry.name.starts_with("/main")));

        let other = dir.join("other.sqlite");
        let (other, _) = registry
            .url_to_fs(
                &format!("db+sqlite://{}/main", other.to_str().unwrap()),
                &FsOptions::new(),
            )
            .unwrap();
        assert!(!Arc::ptr_eq(&fs, &other));

        let (from_options, path) = registry
            .url_to_fs(
                "db+sqlite://localhost/main/users",
                &options(&[("database", ":memory:")]),
            )
            .unwrap();
        assert_eq!(path, "/main/users");
        assert_eq!(from_options.strip_protocol("/main/users"), "/main/users");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn locations_split_like_the_python_filesystems() {
        assert_eq!(sqlite_location("/tmp/a.db/main/t?limit=5"), "/tmp/a.db");
        assert_eq!(sqlite_location("data/A.SQLITE3"), "data/A.SQLITE3");
        assert_eq!(sqlite_location("localhost/main"), "localhost");
        assert_eq!(
            server_location("u@h:5432/app/public/t?limit=5"),
            "u@h:5432/app"
        );
        assert_eq!(
            server_location("h/app?sslmode=require"),
            "h/app?sslmode=require"
        );
        assert_eq!(server_location("/public/t"), "/public");

        let fs = DatabaseFs::new(SqliteDatabase::connect(":memory:").unwrap())
            .with_location("u@h:5432/app");
        assert_eq!(
            fs.strip_protocol("u@h:5432/app/public/t/?limit=5"),
            "/public/t?limit=5"
        );
        assert_eq!(fs.strip_protocol("db+postgres://u@h:5432/app"), "/");
        assert_eq!(
            fs.strip_protocol("/public/t?url=s3://x"),
            "/public/t?url=s3://x"
        );
    }

    #[test]
    fn server_sources_follow_option_precedence() {
        let source = |location: &str, pairs: &[(&str, &str)]| {
            server_source("postgresql", location, &options(pairs)).map(|(source, _)| source)
        };
        assert_eq!(
            source("h:5432/app", &[("url", "postgresql://other/db")]).unwrap(),
            "postgresql://h:5432/app"
        );
        assert_eq!(
            source("h/app", &[("dsn", "postgresql://dsn/db")]).unwrap(),
            "postgresql://dsn/db"
        );
        assert_eq!(
            source(
                "",
                &[
                    ("host", "localhost"),
                    ("port", "55432"),
                    ("dbname", "app"),
                    ("user", "me"),
                    ("password", "p@ss word"),
                    ("sslmode", "disable"),
                ],
            )
            .unwrap(),
            "postgresql://me:p%40ss%20word@localhost:55432/app?sslmode=disable"
        );
        let (mysql, pool) = server_source(
            "mysql",
            "",
            &options(&[
                ("host", "h"),
                ("ssl_mode", "DISABLED"),
                ("charset", "utf8mb4"),
                ("max_pool_size", "4"),
            ]),
        )
        .unwrap();
        assert_eq!(mysql, "mysql://h?ssl-mode=DISABLED&charset=utf8mb4");
        assert_eq!(pool, DbPoolOptions::new(None, Some(4)));
    }

    #[test]
    fn rejects_missing_sources_and_unknown_options() {
        let message = |result: FsResult<SharedFileSystem>| match result {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected an error"),
        };
        assert_eq!(
            message(DatabaseFsFactory::postgres().create("", &FsOptions::new())),
            "invalid argument: db+postgresql requires a DSN or host config"
        );
        assert_eq!(
            message(DatabaseFsFactory::mysql().create("h/app", &options(&[("sslmod", "x")]))),
            "invalid argument: unknown db+mysql option: sslmod"
        );
        assert_eq!(
            message(DatabaseFsFactory::postgres().create("h/app", &options(&[("charset", "x")]))),
            "invalid argument: unknown db+postgresql option: charset"
        );
        assert_eq!(
            message(DatabaseFsFactory::sqlite().create("localhost", &FsOptions::new())),
            "invalid argument: db+sqlite requires a database path or URL"
        );
        assert_eq!(
            message(
                DatabaseFsFactory::postgres()
                    .create("h/app", &options(&[("max_connections", "-1")]))
            ),
            "invalid argument: max_connections must be a non-negative integer, got \"-1\""
        );
    }

    #[test]
    fn server_urls_resolve_against_running_databases() {
        for (variable, protocol) in [
            ("FSSPEC_DB_POSTGRES_URL", "db+postgres"),
            ("FSSPEC_DB_MYSQL_URL", "db+mysql"),
        ] {
            let Ok(url) = std::env::var(variable) else {
                continue;
            };
            let (_, location) = url.split_once("://").unwrap();
            let registry = registry();
            let (fs, path) = registry
                .url_to_fs(&format!("{protocol}://{location}/"), &FsOptions::new())
                .unwrap();
            assert_eq!(path, "/");
            assert!(!fs.ls(&path, true).unwrap().is_empty());
            let (same, _) = registry
                .url_to_fs(&format!("{protocol}://{location}/x"), &FsOptions::new())
                .unwrap();
            assert!(Arc::ptr_eq(&fs, &same));
        }
    }
}
