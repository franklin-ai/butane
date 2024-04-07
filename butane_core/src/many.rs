//! Implementation of many-to-many relationships between models.
#![deny(missing_docs)]
use std::borrow::Cow;

#[cfg(feature = "fake")]
use fake::{Dummy, Faker};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::sync::OnceCell;

use crate::db::{Column, ConnectionMethods};
use crate::query::{BoolExpr, Expr, OrderDirection, Query};
use crate::{DataObject, Error, FieldType, PrimaryKeyType, Result, SqlType, SqlVal, ToSql};

/// Used to implement a many-to-many relationship between models.
///
/// Creates a new table with columns "owner" and "has" If type T has a
/// many-to-many relationship with U, owner type is T::PKType, has is
/// U::PKType. Table name is T_foo_Many where foo is the name of
/// the Many field
//
#[derive(Clone, Debug)]
pub struct Many<T>
where
    T: DataObject,
{
    item_table: Cow<'static, str>,
    owner: Option<SqlVal>,
    owner_type: SqlType,
    new_values: Vec<SqlVal>,
    removed_values: Vec<SqlVal>,
    all_values: OnceCell<Vec<T>>,
}
impl<T> Many<T>
where
    T: DataObject,
{
    /// Constructs a new Many. `init` must be called before it can be
    /// loaded or saved (or those methods will return
    /// `Error::NotInitialized`). `init` will automatically be called
    /// when a [`DataObject`] with a `Many` field is loaded or saved.
    ///
    /// [`DataObject`]: super::DataObject
    pub fn new() -> Self {
        Many {
            item_table: Cow::Borrowed("not_initialized"),
            owner: None,
            owner_type: SqlType::Int,
            new_values: Vec::new(),
            removed_values: Vec::new(),
            all_values: OnceCell::new(),
        }
    }

    /// Used by macro-generated code. You do not need to call this directly.
    pub fn ensure_init(&mut self, item_table: &'static str, owner: SqlVal, owner_type: SqlType) {
        if self.owner.is_some() {
            return;
        }
        self.item_table = Cow::Borrowed(item_table);
        self.owner = Some(owner);
        self.owner_type = owner_type;
        self.all_values = OnceCell::new();
    }

    /// Adds a value. Returns Err(ValueNotSaved) if the
    /// provided value uses automatic primary keys and appears
    /// to have an uninitialized one.
    pub fn add(&mut self, new_val: &T) -> Result<()> {
        // Check for uninitialized pk
        if !new_val.pk().is_valid() {
            return Err(Error::ValueNotSaved);
        }

        // all_values is now out of date, so clear it
        self.all_values = OnceCell::new();
        self.new_values.push(new_val.pk().to_sql());
        Ok(())
    }

    /// Removes a value.
    pub fn remove(&mut self, val: &T) {
        // all_values is now out of date, so clear it
        self.all_values = OnceCell::new();
        self.removed_values.push(val.pk().to_sql())
    }

    /// Returns a reference to the value. It must have already been loaded. If not, returns Error::ValueNotLoaded
    pub fn get(&self) -> Result<impl Iterator<Item = &T>> {
        self.all_values
            .get()
            .ok_or(Error::ValueNotLoaded)
            .map(|v| v.iter())
    }

    // TODO support save and load for sync too
    /// Used by macro-generated code. You do not need to call this directly.
    pub async fn save(&mut self, conn: &impl crate::ConnectionMethods) -> Result<()> {
        let owner = self.owner.as_ref().ok_or(Error::NotInitialized)?;
        while !self.new_values.is_empty() {
            conn.insert_only(
                &self.item_table,
                &self.columns(),
                &[
                    owner.as_ref(),
                    self.new_values.pop().unwrap().as_ref().clone(),
                ],
            )
            .await?;
        }
        if !self.removed_values.is_empty() {
            conn.delete_where(
                &self.item_table,
                BoolExpr::In("has", std::mem::take(&mut self.removed_values)),
            )
            .await?;
        }
        self.new_values.clear();
        Ok(())
    }

    /// Delete all references from the database, and any unsaved additions.
    pub async fn delete(&mut self, conn: &impl ConnectionMethods) -> Result<()> {
        let owner = self.owner.as_ref().ok_or(Error::NotInitialized)?;
        conn.delete_where(
            &self.item_table,
            BoolExpr::Eq("owner", Expr::Val(owner.clone())),
        )
        .await?;
        self.new_values.clear();
        self.removed_values.clear();
        // all_values is now out of date, so clear it
        self.all_values = OnceCell::new();
        Ok(())
    }

    /// Loads the values referred to by this many relationship from the
    /// database if necessary and returns a reference to them.
    pub async fn load(&self, conn: &impl ConnectionMethods) -> Result<impl Iterator<Item = &T>> {
        let query = self.query();
        // If not initialised then there are no values
        let vals: Result<Vec<&T>> = if query.is_err() {
            Ok(Vec::new())
        } else {
            Ok(self.load_query(conn, query.unwrap()).await?.collect())
        };
        vals.map(|v| v.into_iter())
    }

    /// Query the values referred to by this many relationship from the
    /// database if necessary and returns a reference to them.
    fn query(&self) -> Result<Query<T>> {
        let owner: &SqlVal = match &self.owner {
            Some(o) => o,
            None => return Err(Error::NotInitialized),
        };
        Ok(T::query().filter(BoolExpr::Subquery {
            col: T::PKCOL,
            tbl2: self.item_table.clone(),
            tbl2_col: "has",
            expr: Box::new(BoolExpr::Eq("owner", Expr::Val(owner.clone()))),
        }))
    }

    /// Loads the values referred to by this many relationship from a
    /// database query if necessary and returns a reference to them.
    async fn load_query(
        &self,
        conn: &impl ConnectionMethods,
        query: Query<T>,
    ) -> Result<impl Iterator<Item = &T>> {
        let vals: Result<&Vec<T>> = self
            .all_values
            .get_or_try_init(|| async {
                let mut vals = query.load(conn).await?;
                // Now add in the values for things not saved to the db yet
                if !self.new_values.is_empty() {
                    vals.append(
                        &mut T::query()
                            .filter(BoolExpr::In(T::PKCOL, self.new_values.clone()))
                            .load(conn)
                            .await?,
                    );
                }
                Ok(vals)
            })
            .await;
        vals.map(|v| v.iter())
    }

    /// Loads and orders the values referred to by this many relationship from a
    /// database if necessary and returns a reference to them.
    pub async fn load_ordered(
        &self,
        conn: &impl ConnectionMethods,
        order: OrderDirection,
    ) -> Result<impl Iterator<Item = &T>> {
        let query = self.query();
        // If not initialised then there are no values
        let vals: Result<Vec<&T>> = if query.is_err() {
            Ok(Vec::new())
        } else {
            Ok(self
                .load_query(conn, query.unwrap().order(T::PKCOL, order))
                .await?
                .collect())
        };
        vals.map(|v| v.into_iter())
    }

    /// Describes the columns of the Many table
    pub fn columns(&self) -> [Column; 2] {
        [
            Column::new("owner", self.owner_type.clone()),
            Column::new("has", <T::PKType as FieldType>::SQLTYPE),
        ]
    }
}

impl<T> Serialize for Many<T>
where
    T: DataObject + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut serde_state =
            Serializer::serialize_struct(serializer, "Many", false as usize + 1 + 1 + 1)?;
        serde::ser::SerializeStruct::serialize_field(
            &mut serde_state,
            "item_table",
            &self.item_table,
        )?;
        serde::ser::SerializeStruct::serialize_field(&mut serde_state, "owner", &self.owner)?;
        serde::ser::SerializeStruct::serialize_field(
            &mut serde_state,
            "owner_type",
            &self.owner_type,
        )?;
        let default = &Vec::<T>::new();
        let val = self.all_values.get().unwrap_or(default);
        serde::ser::SerializeStruct::serialize_field(&mut serde_state, "all_values", &val)?;
        serde::ser::SerializeStruct::end(serde_state)
    }
}

impl<'de, T> Deserialize<'de> for Many<T>
where
    T: DataObject + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[allow(non_camel_case_types)]
        #[doc(hidden)]
        enum ManyField {
            many_field_0,
            many_field_1,
            many_field_2,
            many_field_3,
            ignore,
        }
        #[doc(hidden)]
        struct FieldVisitor;
        impl<'de> serde::de::Visitor<'de> for FieldVisitor {
            type Value = ManyField;
            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                core::fmt::Formatter::write_str(formatter, "field identifier")
            }
            fn visit_u64<E>(self, value: u64) -> core::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    0u64 => Ok(ManyField::many_field_0),
                    1u64 => Ok(ManyField::many_field_1),
                    2u64 => Ok(ManyField::many_field_2),
                    3u64 => Ok(ManyField::many_field_3),
                    _ => Ok(ManyField::ignore),
                }
            }
            fn visit_str<E>(self, value: &str) -> core::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "item_table" => Ok(ManyField::many_field_0),
                    "owner" => Ok(ManyField::many_field_1),
                    "owner_type" => Ok(ManyField::many_field_2),
                    "all_values" => Ok(ManyField::many_field_3),
                    _ => Ok(ManyField::ignore),
                }
            }
            fn visit_bytes<E>(self, value: &[u8]) -> core::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    b"item_table" => Ok(ManyField::many_field_0),
                    b"owner" => Ok(ManyField::many_field_1),
                    b"owner_type" => Ok(ManyField::many_field_2),
                    b"all_values" => Ok(ManyField::many_field_3),
                    _ => Ok(ManyField::ignore),
                }
            }
        }
        impl<'de> Deserialize<'de> for ManyField {
            #[inline]
            fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Deserializer::deserialize_identifier(deserializer, FieldVisitor)
            }
        }
        #[doc(hidden)]
        struct Visitor<'de, T>
        where
            T: DataObject,
        {
            marker: std::marker::PhantomData<Many<T>>,
            lifetime: std::marker::PhantomData<&'de ()>,
        }
        impl<'de, T> serde::de::Visitor<'de> for Visitor<'de, T>
        where
            T: DataObject + Deserialize<'de>,
        {
            type Value = Many<T>;
            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                core::fmt::Formatter::write_str(formatter, "struct Many")
            }
            // Unused?
            #[inline]
            fn visit_seq<A>(self, mut seq: A) -> core::result::Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let many_field_0 =
                    match serde::de::SeqAccess::next_element::<Cow<'static, str>>(&mut seq)? {
                        Some(value) => value,
                        None => {
                            return Err(serde::de::Error::invalid_length(
                                0usize,
                                &"struct Many with 4 elements",
                            ));
                        }
                    };
                let many_field_1 =
                    match serde::de::SeqAccess::next_element::<Option<SqlVal>>(&mut seq)? {
                        Some(value) => value,
                        None => {
                            return Err(serde::de::Error::invalid_length(
                                1usize,
                                &"struct Many with 4 elements",
                            ));
                        }
                    };
                let many_field_2 = match serde::de::SeqAccess::next_element::<SqlType>(&mut seq)? {
                    Some(value) => value,
                    None => {
                        return Err(serde::de::Error::invalid_length(
                            2usize,
                            &"struct Many with 4 elements",
                        ));
                    }
                };
                let many_field_3 = match serde::de::SeqAccess::next_element::<Vec<T>>(&mut seq)? {
                    Some(value) => value,
                    None => {
                        return Err(serde::de::Error::invalid_length(
                            2usize,
                            &"struct Many with 4 elements",
                        ));
                    }
                };
                let many_field_4 = Default::default();
                let many_field_5 = Default::default();
                Ok(Many {
                    item_table: many_field_0,
                    owner: many_field_1,
                    owner_type: many_field_2,
                    new_values: many_field_4,
                    removed_values: many_field_5,
                    all_values: many_field_3.into(),
                })
            }

            #[inline]
            fn visit_map<A>(self, mut map: A) -> core::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut many_field_0: Option<Cow<'static, str>> = None;
                let mut many_field_1: Option<Option<SqlVal>> = None;
                let mut many_field_2: Option<SqlType> = None;
                let mut many_field_3: Option<Vec<T>> = None;
                while let Some(key) = serde::de::MapAccess::next_key::<ManyField>(&mut map)? {
                    match key {
                        ManyField::many_field_0 => {
                            if Option::is_some(&many_field_0) {
                                return Err(<A::Error as serde::de::Error>::duplicate_field(
                                    "item_table",
                                ));
                            }
                            many_field_0 = Some(serde::de::MapAccess::next_value::<
                                Cow<'static, str>,
                            >(&mut map)?);
                        }
                        ManyField::many_field_1 => {
                            if Option::is_some(&many_field_1) {
                                return Err(<A::Error as serde::de::Error>::duplicate_field(
                                    "owner",
                                ));
                            }
                            many_field_1 = Some(
                                serde::de::MapAccess::next_value::<Option<SqlVal>>(&mut map)?,
                            );
                        }
                        ManyField::many_field_2 => {
                            if Option::is_some(&many_field_2) {
                                return Err(<A::Error as serde::de::Error>::duplicate_field(
                                    "owner_type",
                                ));
                            }
                            many_field_2 =
                                Some(serde::de::MapAccess::next_value::<SqlType>(&mut map)?);
                        }
                        ManyField::many_field_3 => {
                            if Option::is_some(&many_field_3) {
                                return Err(<A::Error as serde::de::Error>::duplicate_field(
                                    "all_values",
                                ));
                            }
                            many_field_3 =
                                Some(serde::de::MapAccess::next_value::<Vec<T>>(&mut map)?);
                        }
                        _ => {
                            let _ = serde::de::MapAccess::next_value::<serde::de::IgnoredAny>(
                                &mut map,
                            )?;
                        }
                    }
                }
                let many_field_0 = match many_field_0 {
                    Some(many_field_0) => many_field_0,
                    None => serde::__private::de::missing_field("item_table")?,
                };
                let many_field_1 = match many_field_1 {
                    Some(many_field_1) => many_field_1,
                    None => serde::__private::de::missing_field("owner")?,
                };
                let many_field_2 = match many_field_2 {
                    Some(many_field_2) => many_field_2,
                    None => serde::__private::de::missing_field("owner_type")?,
                };
                let many_field_3 = match many_field_3 {
                    Some(many_field_3) => {
                        if many_field_3.is_empty() {
                            OnceCell::new()
                        } else {
                            many_field_3.into()
                        }
                    }
                    None => panic!(), // serde::__private::de::missing_field("all_values")?,
                };
                Ok(Many {
                    item_table: many_field_0,
                    owner: many_field_1,
                    owner_type: many_field_2,
                    new_values: Default::default(),
                    removed_values: Default::default(),
                    all_values: many_field_3,
                })
            }
        }
        #[doc(hidden)]
        const FIELDS: &'static [&'static str] =
            &["item_table", "owner", "owner_type", "all_values"];
        Deserializer::deserialize_struct(
            deserializer,
            "Many",
            FIELDS,
            Visitor {
                marker: std::marker::PhantomData::<Many<T>>,
                lifetime: std::marker::PhantomData,
            },
        )
    }
}

impl<T: DataObject> PartialEq<Many<T>> for Many<T> {
    fn eq(&self, other: &Many<T>) -> bool {
        (self.owner == other.owner) && (self.item_table == other.item_table)
    }
}
impl<T: DataObject> Eq for Many<T> {}
impl<T: DataObject> Default for Many<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "fake")]
/// Fake data support is currently limited to empty Many relationships.
impl<T: DataObject> Dummy<Faker> for Many<T> {
    fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &Faker, _rng: &mut R) -> Self {
        Self::new()
    }
}
