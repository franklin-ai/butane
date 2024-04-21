use serde::de::{self, Deserialize, Deserializer, Visitor};
use serde::ser::{Serialize, Serializer};

use std::fmt;
use std::marker::PhantomData;

use derive_more::{Deref, DerefMut, From};

use std::sync::OnceLock as SyncOnceLock;

#[derive(Clone, Debug, Default, Deref, DerefMut, From)]
pub(crate) struct ButaneOnceLock<T>(pub SyncOnceLock<T>);

impl<T> ButaneOnceLock<T> {
    pub fn new() -> Self {
        Self(SyncOnceLock::<T>::new())
    }
}

impl<T> From<T> for ButaneOnceLock<T> {
    fn from(value: T) -> Self {
        Self(SyncOnceLock::from(value))
    }
}

impl<T: Serialize> Serialize for ButaneOnceLock<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.get() {
            Some(val) => serializer.serialize_some(val),
            None => serializer.serialize_none(),
        }
    }
}

struct OnceLockVisitor<T>(PhantomData<*const T>);
impl<'de, T: Deserialize<'de>> Visitor<'de> for OnceLockVisitor<T> {
    type Value = ButaneOnceLock<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an ButaneOnceLock")
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        Ok(ButaneOnceLock::from(SyncOnceLock::from(T::deserialize(
            deserializer,
        )?)))
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(ButaneOnceLock::new())
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for ButaneOnceLock<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_option(OnceLockVisitor(PhantomData))
    }
}
