//! Implementation of foreign key relationships between models.
#![deny(missing_docs)]
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};

#[cfg(feature = "fake")]
use fake::{Dummy, Faker};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::sync::OnceCell;

use crate::{
    AsPrimaryKey, DataObject, Error, FieldType, FromSql, Result, SqlType, SqlVal, SqlValRef, ToSql,
};

/// Used to implement a relationship between models.
///
/// Initialize using `From` or `from_pk`
///
/// # Examples
/// ```ignore
/// #[model]
/// struct Blog {
///   ...
/// }
/// #[model]
/// struct Post {
///   blog: ForeignKey<Blog>,
///   ...
/// }
pub struct ForeignKey<T>
where
    T: DataObject,
{
    // At least one must be initialized (enforced internally by this
    // type), but both need not be
    val: OnceCell<Box<T>>,
    valpk: OnceCell<SqlVal>,
}
impl<T: DataObject> ForeignKey<T> {
    /// Create a value from a reference to the primary key of the value
    pub fn from_pk(pk: T::PKType) -> Self {
        let ret = Self::new_raw();
        ret.valpk.set(pk.into_sql()).unwrap();
        ret
    }
    /// Returns a reference to the value. It must have already been loaded. If not, returns Error::ValueNotLoaded
    pub fn get(&self) -> Result<&T> {
        self.val
            .get()
            .map(|v| v.as_ref())
            .ok_or(Error::ValueNotLoaded)
    }

    /// Returns a reference to the primary key of the value.
    pub fn pk(&self) -> T::PKType {
        match self.val.get() {
            Some(v) => v.pk().clone(),
            None => match self.valpk.get() {
                Some(pk) => T::PKType::from_sql_ref(pk.as_ref()).unwrap(),
                None => panic!("Invalid foreign key state"),
            },
        }
    }

    fn new_raw() -> Self {
        ForeignKey {
            val: OnceCell::new(),
            valpk: OnceCell::new(),
        }
    }

    fn ensure_valpk(&self) -> &SqlVal {
        match self.valpk.get() {
            Some(sqlval) => return sqlval,
            None => match self.val.get() {
                Some(val) => self.valpk.set(val.pk().to_sql()).unwrap(),
                None => panic!("Invalid foreign key state"),
            },
        }
        self.valpk.get().unwrap()
    }
}

// TODO support sync load with ForeignKey too
impl<T: DataObject + Send> ForeignKey<T> {
    /// Loads the value referred to by this foreign key from the
    /// database if necessary and returns a reference to it.
    pub async fn load(&self, conn: &impl crate::ConnectionMethods) -> Result<&T> {
        self.val
            .get_or_try_init(|| async {
                let pk = self.valpk.get().unwrap();
                T::get(conn, &T::PKType::from_sql_ref(pk.as_ref())?)
                    .await
                    .map(Box::new)
            })
            .await
            .map(|v| v.as_ref())
    }
}

impl<T: DataObject> From<T> for ForeignKey<T> {
    fn from(obj: T) -> Self {
        let ret = Self::new_raw();
        ret.val.set(Box::new(obj)).ok();
        ret
    }
}
impl<T: DataObject> From<&T> for ForeignKey<T> {
    fn from(obj: &T) -> Self {
        Self::from_pk(obj.pk().clone())
    }
}
impl<T: DataObject> Clone for ForeignKey<T> {
    fn clone(&self) -> Self {
        // Once specialization lands, it would be nice to clone val if
        // it's clone-able. Then we wouldn't have to ensure the pk
        self.ensure_valpk();
        ForeignKey {
            val: OnceCell::new(),
            valpk: self.valpk.clone(),
        }
    }
}

impl<T> AsPrimaryKey<T> for ForeignKey<T>
where
    T: DataObject,
{
    fn as_pk(&self) -> Cow<T::PKType> {
        Cow::Owned(self.pk())
    }
}

impl<T: DataObject> Eq for ForeignKey<T> {}
impl<T: DataObject> Debug for ForeignKey<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        self.ensure_valpk().fmt(f)
    }
}

impl<T> ToSql for ForeignKey<T>
where
    T: DataObject,
{
    fn to_sql(&self) -> SqlVal {
        self.ensure_valpk().clone()
    }
    fn to_sql_ref(&self) -> SqlValRef<'_> {
        self.ensure_valpk().as_ref()
    }
    fn into_sql(self) -> SqlVal {
        self.ensure_valpk();
        self.valpk.into_inner().unwrap()
    }
}
impl<T> FieldType for ForeignKey<T>
where
    T: DataObject,
{
    const SQLTYPE: SqlType = <T as DataObject>::PKType::SQLTYPE;
    type RefType = <<T as DataObject>::PKType as FieldType>::RefType;
}
impl<T> FromSql for ForeignKey<T>
where
    T: DataObject,
{
    fn from_sql_ref(valref: SqlValRef) -> Result<Self> {
        Ok(ForeignKey {
            valpk: SqlVal::from(valref).into(),
            val: OnceCell::new(),
        })
    }
}
impl<T, U> PartialEq<U> for ForeignKey<T>
where
    U: AsPrimaryKey<T>,
    T: DataObject,
{
    fn eq(&self, other: &U) -> bool {
        match self.val.get() {
            Some(t) => t.pk().eq(&other.as_pk()),
            None => match self.valpk.get() {
                Some(valpk) => valpk.eq(&other.as_pk().to_sql()),
                None => panic!("Invalid foreign key state"),
            },
        }
    }
}

impl<T> Serialize for ForeignKey<T>
where
    T: DataObject + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut serde_state = Serializer::serialize_struct(serializer, "ForeignKey", 2)?;
        let val = self.val.get();
        if let Some(val) = val {
            serde::ser::SerializeStruct::serialize_field(
                &mut serde_state,
                "val",
                val,
            )?;
        }
        let valpk = self.valpk.get();
        if let Some(valpk) = valpk {
            serde::ser::SerializeStruct::serialize_field(
                &mut serde_state,
                "valpk",
                valpk,
            )?;
        }
        serde::ser::SerializeStruct::end(serde_state)
    }
}

impl<'de, T> Deserialize<'de> for ForeignKey<T>
where
    T: DataObject + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[allow(non_camel_case_types)]
        #[doc(hidden)]
        enum FKField {
            field_0,
            field_1,
            ignore,
        }
        #[doc(hidden)]
        struct FieldVisitor;
        impl<'de> serde::de::Visitor<'de> for FieldVisitor {
            type Value = FKField;
            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                core::fmt::Formatter::write_str(formatter, "field identifier")
            }
            fn visit_u64<E>(self, value: u64) -> core::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    0u64 => Ok(FKField::field_0),
                    1u64 => Ok(FKField::field_1),
                    _ => Ok(FKField::ignore),
                }
            }
            fn visit_str<E>(self, value: &str) -> core::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "val" => Ok(FKField::field_0),
                    "valpk" => Ok(FKField::field_1),
                    _ => Ok(FKField::ignore),
                }
            }
            fn visit_bytes<E>(self, value: &[u8]) -> core::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    b"val" => Ok(FKField::field_0),
                    b"valpk" => Ok(FKField::field_1),
                    _ => Ok(FKField::ignore),
                }
            }
        }
        impl<'de> Deserialize<'de> for FKField {
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
            marker: std::marker::PhantomData<ForeignKey<T>>,
            lifetime: std::marker::PhantomData<&'de ()>,
        }
        impl<'de, T> serde::de::Visitor<'de> for Visitor<'de, T>
        where
            T: DataObject + Deserialize<'de>,
        {
            type Value = ForeignKey<T>;
            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                core::fmt::Formatter::write_str(formatter, "struct ForeignKey")
            }
            /*
            #[inline]
            fn visit_seq<A>(self, mut seq: A) -> core::result::Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let field_0 = match serde::de::SeqAccess::next_element::<T>(&mut seq)? {
                    Some(value) => value,
                    None => {
                        return Err(serde::de::Error::invalid_length(
                            0usize,
                            &"struct ForeignKey with 2 elements",
                        ));
                    }
                };
                let field_1 = match serde::de::SeqAccess::next_element::<Option<SqlVal>>(&mut seq)?
                {
                    Some(value) => value,
                    None => {
                        return Err(serde::de::Error::invalid_length(
                            1usize,
                            &"struct ForeignKey with 2 elements",
                        ));
                    }
                };
                Ok(ForeignKey {
                    val: field_0.into(),
                    valpk: field_1.into(),
                })
            }
            */

            #[inline]
            fn visit_map<A>(self, mut map: A) -> core::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut field_0: Option<T> = None;
                let mut field_1: Option<SqlVal> = None;
                while let Some(key) = serde::de::MapAccess::next_key::<FKField>(&mut map)? {
                    match key {
                        FKField::field_0 => {
                            if Option::is_some(&field_0) {
                                return Err(<A::Error as serde::de::Error>::duplicate_field("val"));
                            }
                            field_0 = Some(serde::de::MapAccess::next_value::<T>(&mut map)?);
                        }
                        FKField::field_1 => {
                            if Option::is_some(&field_1) {
                                return Err(<A::Error as serde::de::Error>::duplicate_field(
                                    "valpk",
                                ));
                            }
                            field_1 = Some(serde::de::MapAccess::next_value::<SqlVal>(&mut map)?);
                        }
                        _ => {
                            let _ = serde::de::MapAccess::next_value::<serde::de::IgnoredAny>(
                                &mut map,
                            )?;
                        }
                    }
                }
                /*
                let field_0 = match field_0 {
                    Some(field_0) => field_0,
                    None => serde::__private::de::missing_field("val")?,
                };
                let field_1 = match field_1 {
                    Some(field_1) => field_1,
                    None => serde::__private::de::missing_field("valpk")?,
                };
                */
                match (field_0, field_1) {
                    (Some(field_0), Some(field_1)) => Ok(ForeignKey {
                        val: OnceCell::from(Box::new(field_0)),
                        valpk: field_1.into(),
                    }),
                    _ => Ok(ForeignKey::new_raw())
                }
            }
        }

        #[doc(hidden)]
        const FIELDS: &'static [&'static str] = &["val", "valpk"];
        Deserializer::deserialize_struct(
            deserializer,
            "ForeignKey",
            FIELDS,
            Visitor {
                marker: std::marker::PhantomData::<ForeignKey<T>>,
                lifetime: std::marker::PhantomData,
            },
        )
    }
}

#[cfg(feature = "fake")]
/// Fake data support is currently limited to empty ForeignKey relationships.
impl<T: DataObject> Dummy<Faker> for ForeignKey<T> {
    fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &Faker, _rng: &mut R) -> Self {
        Self::new_raw()
    }
}
