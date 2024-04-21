use serde::de::{self, Deserialize, Deserializer, Visitor};
use serde::ser::{Serialize, Serializer};

use std::fmt;
use std::marker::PhantomData;

use derive_more::{Deref, DerefMut, From};

use tokio::sync::OnceCell as SyncOnceCell;

#[derive(Clone, Debug, Default, Deref, DerefMut, From)]
pub(crate) struct ButaneOnceCell<T>(pub SyncOnceCell<T>);

impl<T> ButaneOnceCell<T> {
    pub fn new() -> Self {
        Self(SyncOnceCell::<T>::new())
    }
}

impl<T> From<T> for ButaneOnceCell<T> {
    fn from(value: T) -> Self {
        Self(SyncOnceCell::from(value))
    }
}

impl<T: Serialize> Serialize for ButaneOnceCell<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.get() {
            Some(val) => serializer.serialize_some(val),
            None => serializer.serialize_none(),
        }
    }
}

struct OnceCellVisitor<T>(PhantomData<*const T>);
impl<'de, T: Deserialize<'de>> Visitor<'de> for OnceCellVisitor<T> {
    type Value = ButaneOnceCell<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an ButaneOnceCell")
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        Ok(ButaneOnceCell::from(SyncOnceCell::from(T::deserialize(
            deserializer,
        )?)))
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(ButaneOnceCell::new())
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for ButaneOnceCell<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_option(OnceCellVisitor(PhantomData))
    }
}
