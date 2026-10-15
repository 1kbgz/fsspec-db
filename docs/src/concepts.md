# Why database objects have filesystem paths

fsspec tools expect directory listings, metadata, and file-like reads. Mapping SQL
objects to those operations lets a browser or data consumer use a database through
an interface it already understands.

## Relations have structure and data

A table or view is directory-like because it contains metadata facets: columns,
indexes, constraints, and view dependencies. Rows are separate virtual files,
selected by an extension on the relation path. This avoids confusing a table's
structure with a particular encoding of its data.

Those files are generated representations rather than stored database objects.
Reading Parquet runs a query and encodes its result. A reported byte size may be
unknown until encoding completes. A small byte read is therefore not necessarily
a cheap database query.

## One mapping supports several database implementations

The Rust `Database` trait owns database primitives. `DatabaseFs` turns them into
filesystem behavior: parsing paths, generating SQL, and encoding data. Python
classes inherit from `AbstractFileSystem` and delegate those primitives through
PyO3, retaining fsspec's surrounding interface.

Native SQL backends use sqlx. Python adapters implement `AbstractDatabase`. They can use the reverse bridge or
the direct `PyDatabaseFileSystem` implementation of the same path model. Metadata depth and type fidelity
still depend on the underlying database and adapter.

## SQL and path reads serve different needs

A relation path expresses a bounded selection from one relation. A SQL query can
express joins, aggregations, or database-specific operations. Both return Arrow
data, but a common representation does not make every database type portable.
Explicit SQL casts can make type conversion predictable across backends.

## Writes and transactions cross another boundary

Writing a data file decodes rows and inserts them into an existing relation.
Overwrite replaces rows; append retains them. Guarded filesystem DDL methods
create, rename, or remove relations separately.

A native write handle commits on successful close and discards on an exceptional
context exit. Deferred fsspec transactions postpone each file commit, but they do
not make several independent file commits one atomic database transaction. Adapter
transaction behavior can also differ from native backends.

See [Paths and metadata](paths.md) for the namespace and
[How to manage tables and metadata caches](how-to-manage.md) for mutation tasks.
