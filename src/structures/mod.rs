pub mod structure_error;

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

use crate::db::db_entry::DBEntry;

use self::structure_error::StructureError;

#[derive(Debug, Builder)]
pub struct HashMapConfig {
    #[builder(default = "1")]
    pub shard_amount: usize,
    #[builder(default = "0")]
    pub capacity: usize,
}

/// A HashMap that is backed by a file.
///
/// This HashMap is fully thread-safe and can be shared across threads.
#[derive(Debug)]
pub struct HashMap<K: Hash + Eq, V> {
    inner: DashMap<K, V>,
    file: Arc<Mutex<File>>,
}

impl<K: Hash + Eq, V> HashMap<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
    V: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
{
    /// Creates a new HashMap with a capacity of 0.
    pub fn new(file: Arc<Mutex<File>>) -> Result<Self, StructureError> {
        let instance = Self {
            inner: DashMap::new(),
            file,
        };
        instance.load_from_file()?;
        Ok(instance)
    }

    /// Creates a new HashMap with a given capacity.
    pub fn with_config(
        file: Arc<Mutex<File>>,
        config: HashMapConfig,
    ) -> Result<Self, StructureError> {
        let instance = Self {
            inner: DashMap::with_capacity_and_shard_amount(config.capacity, config.shard_amount),
            file,
        };
        instance.load_from_file()?;
        Ok(instance)
    }

    fn load_from_file(&self) -> Result<(), StructureError> {
        let mut file = lock_file(&self.file)?;
        file.seek(SeekFrom::Start(0))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let mut cursor = std::io::Cursor::new(buffer);

        while let Ok(entry) = bincode::deserialize_from::<_, DBEntry<K, V>>(&mut cursor) {
            match entry {
                DBEntry::HashMapEntry(key, value) => {
                    self.inner.insert(key, value);
                }
                DBEntry::RemoveHashMapEntry(key) => {
                    self.inner.remove(&key);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Inserts a key-value pair into the HashMap
    ///
    /// Note: Using [`insert_batch`] is more efficient for inserting multiple key-value pairs.
    /// If you use insert, you can consider dropping the returned JoinHandle to improve performance.
    ///
    /// Returns a JoinHandle with a Result containing the old value (None if new) if the operation was successful.
    #[inline]
    pub fn insert(&self, key: K, value: V) -> JoinHandle<Result<Option<V>, StructureError>> {
        let old_value = self.inner.insert(key.clone(), value.clone());
        let file = self.file.clone();
        tokio::spawn(async move {
            let serialized_entry = bincode::serialize(&DBEntry::HashMapEntry(key, value))?;
            serialize_to_file(file, serialized_entry)?;
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
        tokio::spawn(async move {
            let serialized_entries = entries
                .into_iter()
                .map(|(key, value)| bincode::serialize(&DBEntry::HashMapEntry(key, value)))
                .collect::<Result<Vec<_>, _>>()
                .map(|vecs| vecs.into_iter().flatten().collect::<Vec<u8>>())?;
            serialize_to_file(file, serialized_entries)?;
            return Ok(old_values);
        })
    }

    /// Gets a reference to the value corresponding to the given key.
    ///
    /// Returns None if the key does not exist.
    #[inline]
    pub fn get(&self, key: &K) -> Option<ValueRef<'_, K, V>> {
        self.inner.get(key).map(|inner| ValueRef { inner })
    }

    /// Removes a key from the HashMap, returning the value at the key if the key was previously in the HashMap.
    ///
    /// Returns None if the key did not exist.
    pub fn remove(&self, key: &K) -> Option<JoinHandle<Result<Option<V>, StructureError>>> {
        if let Some((key, value)) = self.inner.remove(key) {
            let file = self.file.clone();
            Some(tokio::spawn(async move {
                let data = bincode::serialize(&DBEntry::<K, V>::RemoveHashMapEntry(key))?;
                serialize_to_file(file, data)?;
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
        tokio::spawn(async move {
            let serialized_entries = keys
                .into_iter()
                .map(|key| bincode::serialize(&DBEntry::<K, V>::RemoveHashMapEntry(key)))
                .collect::<Result<Vec<_>, _>>()
                .map(|vecs| vecs.into_iter().flatten().collect::<Vec<u8>>())?;
            serialize_to_file(file, serialized_entries)?;
            Ok(removed_values)
        })
    }

    /// Returns the number of key-value pairs in the HashMap.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the HashMap contains no key-value pairs.
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
        drop(file);
        let mut cursor = std::io::Cursor::new(buffer);
        let mut entries_to_keep = Vec::new();
        while let Ok(entry) = bincode::deserialize_from::<_, DBEntry<K, V>>(&mut cursor) {
            match entry {
                DBEntry::HashMapEntry(_, _) => {}
                DBEntry::RemoveHashMapEntry(_) => {}
                _ => entries_to_keep.push(entry),
            }
        }
        let mut temp_file = tempfile::tempfile()?;
        let serialized_entries = entries_to_keep
            .into_iter()
            .map(|entry| bincode::serialize(&entry))
            .collect::<Result<Vec<_>, _>>()
            .map(|vecs| vecs.into_iter().flatten().collect::<Vec<u8>>())?;
        temp_file.write_all(serialized_entries.as_slice())?;
        temp_file.flush()?;
        let mut file = lock_file(&self.file)?;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        std::io::copy(&mut temp_file, &mut *file)?;
        file.flush()?;
        Ok(())
    }
}

#[inline]
fn lock_file(file: &Arc<Mutex<File>>) -> Result<std::sync::MutexGuard<'_, File>, StructureError> {
    file.lock().map_err(|_| StructureError::MutexLockError)
}

#[inline]
fn serialize_to_file(file: Arc<Mutex<File>>, data: Vec<u8>) -> Result<(), StructureError> {
    let mut file = lock_file(&file)?;
    file.seek(SeekFrom::End(0))?;
    file.write_all(data.as_slice())?;
    file.flush()?;
    Ok(())
}

/// A reference to a value in a HashMap.
pub struct ValueRef<'a, K, V> {
    inner: dashmap::mapref::one::Ref<'a, K, V>,
}

impl<'a, K, V> ValueRef<'a, K, V>
where
    K: Eq + Hash,
{
    /// Returns a reference to the value.
    pub fn value(&self) -> &V {
        self.inner.value()
    }

    /// Returns a reference to the key.
    pub fn key(&self) -> &K {
        self.inner.key()
    }

    /// Returns a reference to the key-value pair.
    pub fn pair(&self) -> (&K, &V) {
        self.inner.pair()
    }

    /// Returns the key-value pair.
    pub fn into_owned(self) -> (K, V)
    where
        K: Clone,
        V: Clone,
    {
        (self.inner.key().clone(), self.inner.value().clone())
    }
}

#[cfg(test)]
mod tests {}
