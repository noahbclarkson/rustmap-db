//! Database entry module for rustmap-db.
//!
//! This module defines the `DBEntry` enum and its serialization/deserialization implementations,
//! which represent the different types of entries that can exist in the database file.

use serde::{
    de::{self, SeqAccess, Visitor},
    ser::SerializeTuple,
    Deserialize, Serialize, Serializer, Deserializer,
};

/// Represents an entry in the database.
///
/// `DBEntry` is an enum that can represent different types of entries within the database,
/// such as a key-value pair in a hashmap, or a key in a hashset, including their removal variants.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DBEntry<K, V> {
    /// Represents a key-value pair entry in a hashmap.
    HashMapEntry(Vec<u8>, K, V),
    /// Represents the removal of a key-value pair from a hashmap.
    RemoveHashMapEntry(Vec<u8>, K),
    /// Represents an entry in a hashset.
    HashSetEntry(Vec<u8>, K),
    /// Represents the removal of an entry from a hashset.
    RemoveHashSetEntry(Vec<u8>, K),
}

impl<K, V> Serialize for DBEntry<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            DBEntry::HashMapEntry(ref id, ref key, ref value) => {
                let mut tuple = serializer.serialize_tuple(4)?;
                tuple.serialize_element(&0u8)?; // 0 indicates HashMapEntry
                tuple.serialize_element(id)?; // id
                tuple.serialize_element(key)?;
                tuple.serialize_element(value)?;
                tuple.end()
            }
            DBEntry::RemoveHashMapEntry(ref id, ref key) => {
                let mut tuple = serializer.serialize_tuple(3)?;
                tuple.serialize_element(&1u8)?; // 1 indicates Remove
                tuple.serialize_element(id)?; // id
                tuple.serialize_element(key)?;
                tuple.end()
            }
            DBEntry::HashSetEntry(ref id, ref key) => {
                let mut tuple = serializer.serialize_tuple(3)?;
                tuple.serialize_element(&2u8)?; // 2 indicates HashSetEntry
                tuple.serialize_element(id)?; // id
                tuple.serialize_element(key)?;
                tuple.end()
            }
            DBEntry::RemoveHashSetEntry(ref id, ref key) => {
                let mut tuple = serializer.serialize_tuple(3)?;
                tuple.serialize_element(&3u8)?; // 3 indicates Remove
                tuple.serialize_element(id)?; // id
                tuple.serialize_element(key)?;
                tuple.end()
            }
        }
    }
}

/// A `Visitor` for deserializing a `DBEntry`.
///
/// `DBEntryVisitor` provides a custom visitor to deserialize `DBEntry` from a sequence
/// of bytes following the structure outlined in the `DBEntry` enum.
struct DBEntryVisitor<K, V> {
    marker: std::marker::PhantomData<fn() -> DBEntry<K, V>>,
}

impl<K, V> DBEntryVisitor<K, V> {
    /// Creates a new `DBEntryVisitor`.
    fn new() -> Self {
        DBEntryVisitor {
            marker: std::marker::PhantomData,
        }
    }
}

impl<'de, K, V> Visitor<'de> for DBEntryVisitor<K, V>
where
    K: Deserialize<'de>,
    V: Deserialize<'de>,
{
    type Value = DBEntry<K, V>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a DBEntry")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let tag: u8 = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        match tag {
            0 => {
                let id = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let key = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let value = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(3, &self))?;
                Ok(DBEntry::HashMapEntry(id, key, value))
            }
            1 => {
                let id = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let key = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                Ok(DBEntry::RemoveHashMapEntry(id, key))
            }
            2 => {
                let id = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let key = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                Ok(DBEntry::HashSetEntry(id, key))
            }
            3 => {
                let id = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let key = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                Ok(DBEntry::RemoveHashSetEntry(id, key))
            }
            _ => Err(de::Error::invalid_value(
                de::Unexpected::Unsigned(tag as u64),
                &self,
            )),
        }
    }
}

impl<'de, K, V> Deserialize<'de> for DBEntry<K, V>
where
    K: Deserialize<'de>,
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &'static [&'static str] = &["tag", "id", "key", "value"];
        deserializer.deserialize_tuple_struct("DBEntry", FIELDS.len(), DBEntryVisitor::new())
    }
}