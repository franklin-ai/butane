//! Not expected to be called directly by most users. Used by code
//! generated by `#[model]`, `query!`, and other macros.

use std::ops::{Deref, DerefMut};
use std::vec::Vec;

use async_trait::async_trait;

use crate::query::{BoolExpr, Expr, Order};
use crate::{Result, SqlType, SqlVal, SqlValRef};

mod internal {
    use super::*;

    /// Methods available on a database connection. Most users do not need
    /// to call these methods directly and will instead use methods on
    /// [DataObject][crate::DataObject] or the `query!` macro. This trait is
    /// implemented by both database connections and transactions.
    #[maybe_async_cfg::maybe(sync(), async())]
    #[async_trait(?Send)]
    pub trait ConnectionMethods {
        async fn execute(&self, sql: &str) -> Result<()>;
        async fn query<'c>(
            &'c self,
            table: &str,
            columns: &[Column],
            expr: Option<BoolExpr>,
            limit: Option<i32>,
            offset: Option<i32>,
            sort: Option<&[Order]>,
        ) -> Result<RawQueryResult<'c>>;
        async fn insert_returning_pk(
            &self,
            table: &str,
            columns: &[Column],
            pkcol: &Column,
            values: &[SqlValRef<'_>],
        ) -> Result<SqlVal>;
        /// Like `insert_returning_pk` but with no return value
        async fn insert_only(
            &self,
            table: &str,
            columns: &[Column],
            values: &[SqlValRef<'_>],
        ) -> Result<()>;
        /// Insert unless there's a conflict on the primary key column, in which case update
        async fn insert_or_replace(
            &self,
            table: &str,
            columns: &[Column],
            pkcol: &Column,
            values: &[SqlValRef<'_>],
        ) -> Result<()>;
        async fn update(
            &self,
            table: &str,
            pkcol: Column,
            pk: SqlValRef<'_>,
            columns: &[Column],
            values: &[SqlValRef<'_>],
        ) -> Result<()>;
        async fn delete(&self, table: &str, pkcol: &'static str, pk: SqlVal) -> Result<()> {
            self.delete_where(table, BoolExpr::Eq(pkcol, Expr::Val(pk)))
                .await?;
            Ok(())
        }
        async fn delete_where(&self, table: &str, expr: BoolExpr) -> Result<usize>;
        /// Tests if a table exists in the database.
        async fn has_table(&self, table: &str) -> Result<bool>;
    }
}

pub use internal::ConnectionMethodsAsync as ConnectionMethods;

pub trait ConnectionMethodWrapper {
    type Wrapped: ConnectionMethods;
    fn wrapped_connection_methods(&self) -> Result<&Self::Wrapped>;
}

pub mod sync {
    use super::*;
    pub use internal::ConnectionMethodsSync as ConnectionMethods;
}

/// Represents a database column. Most users do not need to use this
/// directly.
#[derive(Debug)]
pub struct Column {
    name: &'static str,
    ty: SqlType,
}
impl Column {
    pub const fn new(name: &'static str, ty: SqlType) -> Self {
        Column { name, ty }
    }
    pub fn name(&self) -> &'static str {
        self.name
    }
    pub fn ty(&self) -> &SqlType {
        &self.ty
    }
}

/// Backend-specific row abstraction. Only implementors of new
/// backends need use this trait directly.
pub trait BackendRow {
    fn get(&self, idx: usize, ty: SqlType) -> Result<SqlValRef>;
    fn len(&self) -> usize;
    // clippy wants this method to exist
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Abstraction of rows returned from a query. Most users do not need
/// to deal with this directly and should use the `query!` macro or
/// [Query](crate::query::Query) type.
pub trait BackendRows {
    // Advance to the next item and get it
    fn next<'a>(&'a mut self) -> Result<Option<&'a (dyn BackendRow + 'a)>>;
    // Get the item most recently returned by next
    fn current<'a>(&'a self) -> Option<&'a (dyn BackendRow + 'a)>;
    #[inline]
    fn mapped<F, B>(self, f: F) -> MapDeref<Self, F>
    where
        Self: Sized,
        F: FnMut(&(dyn BackendRow)) -> Result<B>,
    {
        MapDeref { it: self, f }
    }
}

#[derive(Debug)]
pub struct MapDeref<I, F> {
    it: I,
    f: F,
}

impl<I, F, B> fallible_iterator::FallibleIterator for MapDeref<I, F>
where
    I: BackendRows,
    F: FnMut(&(dyn BackendRow)) -> Result<B>,
{
    type Item = B;
    type Error = crate::Error;

    #[inline]
    fn next(&mut self) -> Result<Option<Self::Item>> {
        match self.it.next() {
            Ok(Some(v)) => (self.f)(v).map(Some),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub type RawQueryResult<'a> = Box<dyn BackendRows + 'a>;
pub type QueryResult<T> = Vec<T>;

#[derive(Debug)]
pub(crate) struct VecRows<T> {
    rows: Vec<T>,
    idx: usize,
}
impl<T> VecRows<T> {
    #[allow(unused)] // Not used with all feature combinations
    pub fn new(rows: Vec<T>) -> Self {
        VecRows { rows, idx: 0 }
    }
}
#[cfg(feature = "async-adapter")]
pub(crate) fn vec_from_backend_rows<'a>(
    mut other: Box<dyn BackendRows + 'a>,
    columns: &[Column],
) -> Result<VecRows<VecRow>> {
    let mut rows: Vec<VecRow> = Vec::new();
    while let Some(row) = other.next()? {
        rows.push(VecRow::new(row, columns)?)
    }
    Ok(VecRows::new(rows))
}
impl<T> BackendRows for VecRows<T>
where
    T: BackendRow,
{
    fn next(&mut self) -> Result<Option<&(dyn BackendRow)>> {
        let ret = self.rows.get(self.idx);
        self.idx += 1;
        Ok(ret.map(|row| row as &dyn BackendRow))
    }

    fn current(&self) -> Option<&(dyn BackendRow)> {
        self.rows.get(self.idx).map(|row| row as &dyn BackendRow)
    }
}

impl<'a> BackendRows for Box<dyn BackendRows + 'a> {
    fn next(&mut self) -> Result<Option<&(dyn BackendRow)>> {
        BackendRows::next(self.deref_mut())
    }

    fn current(&self) -> Option<&(dyn BackendRow)> {
        self.deref().current()
    }
}

#[derive(Debug)]
pub(crate) struct VecRow {
    values: Vec<SqlVal>,
}

#[cfg(feature = "async-adapter")]
impl VecRow {
    fn new(original: &(dyn BackendRow), columns: &[Column]) -> Result<Self> {
        if original.len() != columns.len() {
            return Err(crate::Error::BoundsError(
                "row length doesn't match columns specifier length".into(),
            ));
        }
        Ok(Self {
            values: (0..(columns.len()))
                .map(|i| {
                    original
                        .get(i, columns[i].ty.clone())
                        .map(|valref| valref.into())
                })
                .collect::<Result<Vec<SqlVal>>>()?,
        })
    }
}
impl BackendRow for VecRow {
    fn get(&self, idx: usize, ty: SqlType) -> Result<SqlValRef> {
        self.values
            .get(idx)
            .ok_or_else(|| crate::Error::BoundsError("idx out of bounds".into()))
            .and_then(|val| {
                if val.is_compatible(&ty, true) {
                    Ok(val)
                } else {
                    Err(crate::Error::CannotConvertSqlVal(ty.clone(), val.clone()))
                }
            })
            .map(|val| val.as_ref())
    }
    fn len(&self) -> usize {
        self.values.len()
    }
}
