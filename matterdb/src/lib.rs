//! Interfaces to work with persisted data.
//!
//! # Database
//!
//! A [`Database`] is a container for data persistence. Internally, a `Database` is
//! a collection of named key-value stores (aka column families)
//! with reading isolation and atomic writes. The database is assumed to be embedded,
//! that is, the application process has exclusive access to the DB during operation.
//! You can interact with the `Database` from multiple threads by cloning its instance.
//!
//! This crate provides two database types: [`RocksDB`] and [`TemporaryDB`].
//!
//! # Snapshot and Fork
//!
//! Snapshots and forks facilitate access to the database.
//!
//! If you need to read the data, you can create a [`Snapshot`] using the [`snapshot()`][Database::snapshot()] method
//! of the `Database` instance. Snapshots provide read isolation, so you are guaranteed to work
//! with consistent values even if the data in the database changes between reads. `Snapshot`
//! provides all the necessary methods for reading data from the database, so `&Snapshot`
//! is used as a storage view for creating a read-only representation of the [indexes](#indexes).
//!
//! If you need to make changes to the database, you need to create a [`Fork`] using
//! the [`fork()`][Database::fork()] method of the `Database`. Like `Snapshot`, `Fork` provides read isolation,
//! but also allows creating a sequence of changes to the database that are specified
//! as a [`Patch`]. A patch can be atomically [`merge`](Database::merge())d into a database. Different threads
//! may call `merge` concurrently.
//!
//! # `BinaryKey` and `BinaryValue` traits
//!
//! If you need to use your own data types as keys or values in the storage, you need to implement
//! the [`BinaryKey`] or [`BinaryValue`] traits respectively. These traits have already been
//! implemented for most standard types.
//!
//! # Indexes
//!
//! Indexes are structures representing data collections stored in the database.
//! This concept is similar to tables in relational databases. The interfaces
//! of the indexes are similar to ordinary collections (like arrays, maps and sets).
//!
//! Each index occupies a certain set of keys in a single column family of the [`Database`].
//! On the other hand, multiple indexes can be stored in the same column family, provided
//! that their key spaces do not intersect. Isolation is commonly achieved with the help
//! of [`Group`]s or keyed [`IndexAddress`]es.
//!
//! This crate provides the following index types:
//!
//! - [`Entry`] is a specific index that stores only one value. Useful for global values, such as
//!   configuration. Similar to a combination of [`Box`] and [`Option`].
//! - [`ListIndex`] is a list of items stored in a sequential order. Similar to [`Vec`].
//! - [`SparseListIndex`] is a list of items stored in a sequential order. Similar to `ListIndex`,
//!   but may contain indexes without elements.
//! - [`MapIndex`] is a map of keys and values. Similar to [`BTreeMap`](std::collections::BTreeMap).
//! - [`KeySetIndex`] is a set of items, similar to [`BTreeSet`](std::collections::BTreeSet).
//!
//! # Migrations
//!
//! The database [provides tooling](migration) for data migrations. With the help
//! of migration, it is possible to gradually accumulate changes to a set of indexes (including
//! across process restarts) and then atomically apply or discard these changes.

// Re-exports for use in the derive macros.
#[doc(hidden)]
pub mod _reexports {
    pub use anyhow::Error;
}

// Workaround for 'Linked file at path {matterdb_path}/struct.MapIndex.html
// does not exist!'
#[doc(no_inline)]
pub use self::indexes::{Entry, Group, KeySetIndex, ListIndex, MapIndex, SparseListIndex};
pub use self::{
    backends::{
        rocksdb::{self, RocksDB},
        temporarydb::TemporaryDB,
    },
    db::{BoxedIterator, Database, Fork, Iterator, Patch, ReadonlyFork, Snapshot},
    error::Error,
    keys::BinaryKey,
    options::DBOptions,
    values::BinaryValue,
    views::{IndexAddress, IndexType, ResolvedAddress},
};

#[macro_use]
mod macros;
pub mod access;
mod backends;
mod db;
mod error;
pub mod indexes;
mod keys;
pub mod migration;
mod options;
pub mod validation;
mod values;
mod views;

/// A specialized `Result` type for I/O operations with storage.
pub type Result<T> = std::result::Result<T, Error>;
