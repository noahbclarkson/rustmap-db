//! Database entry module for rustmap-db.
//!
//! This module defines the `DBEntry` enum and its serialization/deserialization implementations,
//! which represent the different types of entries that can exist in the database file.

use serde::{
    de::{self, SeqAccess, Visitor},
    ser::SerializeTuple,
    Deserialize, Deserializer, Serialize, Serializer,
};

/// Represents an entry in the database.
///
/// `DBEntry` is an enum that can represent different types of entries within the database,
/// such as a key-value pair in a hashmap, or a key in a hashset, including their removal variants.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DBEntry {
    /// Represents a key-value pair entry in a hashmap.
    HashMapEntry(Vec<u8>, Vec<u8>, Vec<u8>),
    /// Represents the removal of a key-value pair from a hashmap.
    RemoveHashMapEntry(Vec<u8>, Vec<u8>),
    /// Represents an entry in a hashset.
    HashSetEntry(Vec<u8>, Vec<u8>),
    /// Represents the removal of an entry from a hashset.
    RemoveHashSetEntry(Vec<u8>, Vec<u8>),
}

impl Serialize for DBEntry {
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
struct DBEntryVisitor {
    marker: std::marker::PhantomData<fn() -> DBEntry>,
}

impl DBEntryVisitor {
    /// Creates a new `DBEntryVisitor`.
    fn new() -> Self {
        DBEntryVisitor {
            marker: std::marker::PhantomData,
        }
    }
}

impl<'de> Visitor<'de> for DBEntryVisitor {
    type Value = DBEntry;

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

impl<'de> Deserialize<'de> for DBEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &'static [&'static str] = &["tag", "id", "key", "value"];
        deserializer.deserialize_tuple_struct("DBEntry", FIELDS.len(), DBEntryVisitor::new())
    }
}

#[cfg(test)]
mod db_entry_tests {
    use super::*;

    // Helper function to serialize a DBEntry
    fn serialize_entry(entry: &DBEntry) -> Vec<u8> {
        bincode::serialize(entry).expect("Serialization should succeed")
    }

    // Helper function to deserialize a DBEntry
    fn deserialize_entry(data: &[u8]) -> DBEntry {
        bincode::deserialize(data).expect("Deserialization should succeed")
    }

    #[test]
    fn test_serialize_deserialize_hashmap_entry() {
        let entry = DBEntry::HashMapEntry(vec![1], vec![2], vec![3]);
        let serialized = serialize_entry(&entry);
        let deserialized = deserialize_entry(&serialized);
        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_serialize_deserialize_remove_hashmap_entry() {
        let entry = DBEntry::RemoveHashMapEntry(vec![1], vec![2]);
        let serialized = serialize_entry(&entry);
        let deserialized = deserialize_entry(&serialized);
        assert_eq!(entry, deserialized);
    }

    #[test]
    #[should_panic(expected = "Deserialization should succeed")]
    fn test_deserialization_failure() {
        // Create a byte array that does not correspond to any valid DBEntry
        let data = vec![255, 255, 255];
        let _entry: DBEntry = bincode::deserialize(&data).expect("Deserialization should succeed");
    }

    #[test]
    fn test_incorrect_format_deserialization() {
        // Create a byte array with an incorrect format
        let data = vec![10, 20, 30]; // Intentionally incorrect format
        let result = bincode::deserialize::<DBEntry>(&data);
        assert!(
            result.is_err(),
            "Deserialization should fail due to incorrect format"
        );
    }

    #[test]
    fn test_large_data_serialization_deserialization() {
        let large_vec = vec![0u8; 10000]; // A large data vector
        let entry = DBEntry::HashMapEntry(large_vec.clone(), large_vec.clone(), large_vec.clone());

        let serialized =
            bincode::serialize(&entry).expect("Serialization should succeed with large data");
        let deserialized: DBEntry = bincode::deserialize(&serialized)
            .expect("Deserialization should succeed with large data");

        match deserialized {
            DBEntry::HashMapEntry(id, key, value) => {
                assert_eq!(id, large_vec);
                assert_eq!(key, large_vec);
                assert_eq!(value, large_vec);
            }
            _ => panic!("Deserialized to incorrect entry type"),
        }
    }
}
