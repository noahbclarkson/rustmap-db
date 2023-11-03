use serde::{
    de::{self, SeqAccess, Visitor},
    ser::SerializeTuple,
    Deserialize, Serialize, Serializer, Deserializer,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DBEntry<K, V> {
    HashMapEntry(K, V),
    RemoveHashMapEntry(K),
    HashSetEntry(K),
    RemoveHashSetEntry(K),  
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
            DBEntry::HashMapEntry(ref key, ref value) => {
                let mut tuple = serializer.serialize_tuple(3)?;
                tuple.serialize_element(&0u8)?; // 0 indicates HashMapEntry
                tuple.serialize_element(key)?;
                tuple.serialize_element(value)?;
                tuple.end()
            }
            DBEntry::RemoveHashMapEntry(ref key) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&1u8)?; // 1 indicates Remove
                tuple.serialize_element(key)?;
                tuple.end()
            }
            DBEntry::HashSetEntry(ref key) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&2u8)?; // 2 indicates HashSetEntry
                tuple.serialize_element(key)?;
                tuple.end()
            }
            DBEntry::RemoveHashSetEntry(ref key) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&3u8)?; // 3 indicates Remove
                tuple.serialize_element(key)?;
                tuple.end()
            }
        }
    }
}

struct DBEntryVisitor<K, V> {
    marker: std::marker::PhantomData<fn() -> DBEntry<K, V>>,
}

impl<K, V> DBEntryVisitor<K, V> {
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
                let key = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let value = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                Ok(DBEntry::HashMapEntry(key, value))
            }
            1 => {
                let key = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(DBEntry::RemoveHashMapEntry(key))
            }
            2 => {
                let key = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(DBEntry::HashSetEntry(key))
            }
            3 => {
                let key = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(DBEntry::RemoveHashSetEntry(key))
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
        const FIELDS: &'static [&'static str] = &["tag", "key", "value"];
        deserializer.deserialize_tuple_struct("DBEntry", FIELDS.len(), DBEntryVisitor::new())
    }
}
