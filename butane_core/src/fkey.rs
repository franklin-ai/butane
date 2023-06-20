use crate::db::ConnectionMethods;
use crate::*;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[cfg(feature = "fake")]
use fake::{Dummy, Faker};

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
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ForeignKey<T>
where
    T: DataObject + Clone,
{
    // At least one must be initialized (enforced internally by this
    // type), but both need not be
    val: OnceCell<Box<T>>,
    valpk: OnceCell<SqlVal>,
}
impl<T: DataObject + Clone> ForeignKey<T> {
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

    /// Loads the value referred to by this foreign key from the
    /// database if necessary and returns a reference to it.
    pub fn load(&self, conn: &impl ConnectionMethods) -> Result<&T> {
        self.val
            .get_or_try_init(|| {
                let pk = self.valpk.get().unwrap();
                T::get(conn, &T::PKType::from_sql_ref(pk.as_ref())?).map(Box::new)
            })
            .map(|v| v.as_ref())
    }

    fn new_raw() -> Self {
        ForeignKey {
            val: OnceCell::new(),
            valpk: OnceCell::new(),
        }
    }

    pub fn ensure_valpk(&self) -> &SqlVal {
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

impl<T: DataObject + Clone> From<T> for ForeignKey<T> {
    fn from(obj: T) -> Self {
        let ret = Self::new_raw();
        ret.val.set(Box::new(obj)).ok();
        ret
    }
}
impl<T: DataObject + Clone> From<&T> for ForeignKey<T> {
    fn from(obj: &T) -> Self {
        let ret = Self::new_raw();
        ret.val.set(Box::new(obj.clone())).ok();
        ret
    }
}

impl<T> AsPrimaryKey<T> for ForeignKey<T>
where
    T: DataObject + Clone,
{
    fn as_pk(&self) -> Cow<T::PKType> {
        Cow::Owned(self.pk())
    }
}

impl<T: DataObject + Clone> Eq for ForeignKey<T> {}

impl<T> ToSql for ForeignKey<T>
where
    T: DataObject + Clone,
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
    T: DataObject + Clone,
{
    const SQLTYPE: SqlType = <T as DataObject>::PKType::SQLTYPE;
    type RefType = <<T as DataObject>::PKType as FieldType>::RefType;
}
impl<T> FromSql for ForeignKey<T>
where
    T: DataObject + Clone,
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
    T: DataObject + Clone,
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

#[cfg(feature = "fake")]
/// Fake data support is currently limited to empty ForeignKey relationships.
impl<T: DataObject + Clone> Dummy<Faker> for ForeignKey<T> {
    fn dummy_with_rng<R: rand::Rng + ?Sized>(_: &Faker, _rng: &mut R) -> Self {
        Self::new_raw()
    }
}
