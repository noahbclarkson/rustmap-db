use dashmap::DashMap;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    hash::Hash,
    io::{Read, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
};
use tokio::task::JoinHandle;

use crate::{db::db_entry::DBEntry, StructureError};

use super::{lock_file, serialize_to_file, value_ref::ValueRefPair};

/// Configuration for creating a `HashMap`.
///
/// This struct defines the parameters for creating a `HashMap`, such as
/// the number of shards and the initial capacity.
#[derive(Debug, Builder)]
pub struct HashMapConfig {
    #[builder(default = "1")]
    pub shard_amount: usize,
    #[builder(default = "0")]
    pub capacity: usize,
}

/// A file-backed, thread-safe hashmap structure.
///
/// `HashMap` provides a persistent, concurrent key-value store that is backed by a file.
/// It supports operations like `insert`, `get`, and `remove`, with changes being
/// written to disk.
#[derive(Debug)]
pub struct HashMap<K: Hash + Eq, V> {
    inner: DashMap<K, V>,
    file: Arc<Mutex<File>>,
    id: Vec<u8>,
}

impl<K: Hash + Eq, V> HashMap<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
    V: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
{
    /// Creates a new HashMap with a capacity of 0.
    pub fn new(file: Arc<Mutex<File>>, id: Vec<u8>) -> Result<Self, StructureError> {
        let instance = Self {
            inner: DashMap::new(),
            file,
            id: bincode::serialize(&id)?,
        };
        instance.load_from_file()?;
        Ok(instance)
    }

    /// Creates a new HashMap with a given capacity.
    pub fn with_config(
        file: Arc<Mutex<File>>,
        id: Vec<u8>,
        config: HashMapConfig,
    ) -> Result<Self, StructureError> {
        let instance = Self {
            inner: DashMap::with_capacity_and_shard_amount(config.capacity, config.shard_amount),
            file,
            id,
        };
        instance.load_from_file()?;
        Ok(instance)
    }

    /// Loads the hash set contents from the file.
    ///
    /// Internal function used during initialization to load the set's state from the file.
    fn load_from_file(&self) -> Result<(), StructureError> {
        let mut file = lock_file(&self.file)?;
        file.seek(SeekFrom::Start(0))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let mut cursor = std::io::Cursor::new(&buffer);

        while cursor.position() < buffer.len() as u64 {
            match bincode::deserialize_from::<_, DBEntry>(&mut cursor) {
                Ok(entry) => match entry {
                    DBEntry::HashMapEntry(id, key, value) => {
                        if id == self.id {
                            let key = bincode::deserialize::<K>(&key)?;
                            let value = bincode::deserialize::<V>(&value)?;
                            self.inner.insert(key, value);
                        }
                    }
                    DBEntry::RemoveHashMapEntry(id, key) => {
                        if id == self.id {
                            let key = bincode::deserialize::<K>(&key)?;
                            self.inner.remove(&key);
                        }
                    }
                    _ => {}
                },
                Err(e) => match e.as_ref() {
                    bincode::ErrorKind::Io(e) => {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                    _ => {
                        return Err(StructureError::BinCodeError(e));
                    }
                },
            }
        }

        Ok(())
    }

    /// Inserts a key-value pair into the HashMap
    ///
    /// Note: Using [`insert_batch`] is more efficient for inserting multiple key-value pairs.
    ///
    /// If you use insert, you can consider dropping the returned JoinHandle to improve performance.
    /// However, you can't be sure that the operation was successful if you do so.
    ///
    /// As a compromise you can try awaiting the JoinHandle later in your code if you don't need the result immediately.
    ///
    /// [`insert_batch`]: #method.insert_batch
    ///
    /// Returns a JoinHandle with a Result containing the old value (None if new) if the operation was successful.
    #[inline]
    pub fn insert(&self, key: K, value: V) -> JoinHandle<Result<Option<V>, StructureError>> {
        let old_value = self.inner.insert(key.clone(), value.clone());
        let file = self.file.clone();
        let id = self.id.clone();
        tokio::spawn(async move {
            let key = bincode::serialize(&key)?;
            let value = bincode::serialize(&value)?;
            serialize_to_file(&DBEntry::HashMapEntry(id.clone(), key, value), &file)?;
            Ok(old_value)
        })
    }

    /// Inserts a batch of key-value pairs into the HashMap.
    ///
    /// Returns a JoinHandle that can be awaited to wait for the operation to complete.
    ///
    /// JoinHandle will return a Result containing a Vec of the old values (None if new) if the operation was successful.
    pub fn insert_batch(
        &self,
        entries: Vec<(K, V)>,
    ) -> JoinHandle<Result<Vec<Option<V>>, StructureError>> {
        let mut old_values = Vec::with_capacity(entries.len());
        for (key, value) in &entries {
            old_values.push(self.inner.insert(key.clone(), value.clone()));
        }

        let file = self.file.clone();
        let id = self.id.clone();
        tokio::spawn(async move {
            let entries = entries
                .into_iter()
                .map(|(key, value)| {
                    let key = bincode::serialize(&key)?;
                    let value = bincode::serialize(&value)?;
                    Ok(DBEntry::HashMapEntry(id.clone(), key, value))
                })
                .collect::<Result<Vec<DBEntry>, StructureError>>()?;
            serialize_to_file(&entries, &file)?;
            return Ok(old_values);
        })
    }

    /// Gets a reference to the value corresponding to the given key.
    ///
    /// Returns None if the key does not exist.
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<ValueRefPair<'_, K, V>> {
        self.inner.get(key).map(|inner| ValueRefPair::new(inner))
    }

    /// Removes a key from the HashMap, returning the value at the key if the key was previously in the HashMap.
    ///
    /// Returns None if the key did not exist.
    pub fn remove(&self, key: &K) -> Option<JoinHandle<Result<Option<V>, StructureError>>> {
        if let Some((key, value)) = self.inner.remove(key) {
            let file = self.file.clone();
            let id = self.id.clone();
            Some(tokio::spawn(async move {
                let key = bincode::serialize(&key)?;
                serialize_to_file(&DBEntry::RemoveHashMapEntry(id.clone(), key), &file)?;
                Ok(Some(value))
            }))
        } else {
            None
        }
    }

    /// Removes a batch of keys from the HashMap.
    ///
    /// Returns a JoinHandle that can be awaited to wait for the operation to complete.
    ///
    /// JoinHandle will return a Result containing a Vec of the removed key-value pairs if the operation was successful.
    pub fn remove_batch(&self, keys: Vec<K>) -> JoinHandle<Result<Vec<(K, V)>, StructureError>> {
        let mut removed_values = Vec::with_capacity(keys.len());
        for key in &keys {
            if let Some((key, value)) = self.inner.remove(key) {
                removed_values.push((key, value));
            }
        }

        let file = self.file.clone();
        let id = self.id.clone();
        tokio::spawn(async move {
            let entries = removed_values
                .clone()
                .into_iter()
                .map(|(key, _)| {
                    let key = bincode::serialize(&key)?;
                    Ok(DBEntry::RemoveHashMapEntry(id.clone(), key))
                })
                .collect::<Result<Vec<DBEntry>, StructureError>>()?;
            serialize_to_file(&entries, &file)?;
            Ok(removed_values)
        })
    }

    /// Returns the number of key-value pairs in the HashMap.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the HashMap contains no key-value pairs.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Clears the HashMap, removing all key-value pairs.
    ///
    /// Returns a Result containing () if the operation was successful.
    ///
    /// This function is thread-safe since it locks the file and uses a temporary file for writing.
    pub fn clear(&self) -> Result<(), StructureError> {
        self.inner.clear();
        let mut file = lock_file(&self.file)?;
        file.seek(SeekFrom::Start(0))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let mut cursor = std::io::Cursor::new(buffer);
        let mut entries_to_keep = Vec::new();
        while let Ok(entry) = bincode::deserialize_from::<_, DBEntry>(&mut cursor) {
            match entry.clone() {
                DBEntry::HashMapEntry(id, _, _) => {
                    if id != self.id {
                        entries_to_keep.push(entry);
                    }
                }
                DBEntry::RemoveHashMapEntry(id, _) => {
                    if id != self.id {
                        entries_to_keep.push(entry);
                    }
                }
                _ => entries_to_keep.push(entry),
            }
        }
        let serialized_entries = entries_to_keep
            .into_iter()
            .map(|entry| bincode::serialize(&entry))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<u8>>();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(serialized_entries.as_slice())?;
        file.flush()?;
        Ok(())
    }

    /// Returns the capacity of the HashMap.
    ///
    /// The capacity is the number of key-value pairs that the HashMap can hold without reallocating memory.
    ///
    /// Note that the capacity is not a bound on the HashMap's size.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}
