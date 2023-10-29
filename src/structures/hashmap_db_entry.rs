
use serde::{Serialize, Deserialize, Serializer, ser::SerializeTuple, de::{Visitor, SeqAccess, self}, Deserializer};


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HashMapDBEntry<K, V> {
    Insert(K, V),
    Remove(K),
}

impl<K, V> Serialize for HashMapDBEntry<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            HashMapDBEntry::Insert(ref key, ref value) => {
                let mut tuple = serializer.serialize_tuple(3)?;
                tuple.serialize_element(&0u8)?; // 0 indicates Insert
                tuple.serialize_element(key)?;
                tuple.serialize_element(value)?;
                tuple.end()
            }
            HashMapDBEntry::Remove(ref key) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&1u8)?; // 1 indicates Remove
                tuple.serialize_element(key)?;
                tuple.end()
            }
        }
    }
}

struct HashMapDBEntryVisitor<K, V> {
    marker: std::marker::PhantomData<fn() -> HashMapDBEntry<K, V>>,
}

impl<K, V> HashMapDBEntryVisitor<K, V> {
    fn new() -> Self {
        HashMapDBEntryVisitor {
            marker: std::marker::PhantomData,
        }
    }
}

impl<'de, K, V> Visitor<'de> for HashMapDBEntryVisitor<K, V>
where
    K: Deserialize<'de>,
    V: Deserialize<'de>,
{
    type Value = HashMapDBEntry<K, V>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a HashMapDBEntry")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let tag: u8 = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
        match tag {
            0 => {
                let key = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let value = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(2, &self))?;
                Ok(HashMapDBEntry::Insert(key, value))
            }
            1 => {
                let key = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(HashMapDBEntry::Remove(key))
            }
            _ => Err(de::Error::invalid_value(de::Unexpected::Unsigned(tag as u64), &self)),
        }
    }
}


impl<'de, K, V> Deserialize<'de> for HashMapDBEntry<K, V>
where
    K: Deserialize<'de>,
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &'static [&'static str] = &["tag", "key", "value"];
        deserializer.deserialize_tuple_struct("DBEntry", FIELDS.len(), HashMapDBEntryVisitor::new())
    }
}





