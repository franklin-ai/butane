use super::adb::{ATable, DeferredSqlType, TypeKey, ADB};
use crate::{db, Result};
use std::borrow::Cow;
use std::cmp::PartialEq;

/// Type representing a database migration. A migration describes how
/// to bring the database from state A to state B. In general, the
/// methods on this type are persistent -- they read from and write to
/// the filesystem.
///
/// A Migration cannot be constructed directly, only retrieved from
/// [Migrations][crate::migrations::Migrations].
pub trait Migration: PartialEq {
    /// Retrieves the full abstract database state describing all tables
    fn db(&self) -> Result<ADB>;

    /// Get the migration before this one (if any).
    fn migration_from(&self) -> Result<Option<Self>>
    where
        Self: Sized;

    /// The name of this migration.
    fn name(&self) -> Cow<str>;

    /// Apply the migration to a database connection. The connection
    /// must be for the same type of database as
    /// [create_migration][crate::migrations::Migrations::create_migration]
    /// and the database must be in the state of the migration prior
    /// to this one ([from_migration][crate::migrations::Migration::from_migration])
    fn apply(&self, conn: &mut impl db::BackendConnection) -> Result<()>;
}

/// A migration which can be modified
pub trait MigrationMut: Migration {
    /// Adds an abstract table to the migration. The table state should
    /// represent the expected state after the migration has been
    /// applied. It is expected that all tables will be added to the
    /// migration in this fashion.
    fn write_table(&mut self, table: &ATable) -> Result<()>;

    /// Adds a TypeKey -> SqlType mapping. Only meaningful on the special current migration.
    fn add_type(&self, key: TypeKey, sqltype: DeferredSqlType) -> Result<()>;
}
