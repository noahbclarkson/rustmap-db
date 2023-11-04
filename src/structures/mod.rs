pub mod structure_error;
pub mod value_ref;

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

use self::{structure_error::StructureError, value_ref::ValueRef};

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
            id,
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

    fn load_from_file(&self) -> Result<(), StructureError> {
        let mut file = lock_file(&self.file)?;
        file.seek(SeekFrom::Start(0))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let mut cursor = std::io::Cursor::new(buffer);

        while let Ok(entry) = bincode::deserialize_from::<_, DBEntry<K, V>>(&mut cursor) {
            match entry {
                DBEntry::HashMapEntry(id, key, value) => {
                    if id == self.id {
                        self.inner.insert(key, value);
                    }
                }
                DBEntry::RemoveHashMapEntry(id, key) => {
                    if id == self.id {
                        self.inner.remove(&key);
                    }
                }
                _ => {}
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
            let serialized_entry =
                bincode::serialize(&DBEntry::HashMapEntry(id.clone(), key, value))?;
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
        let id = self.id.clone();
        tokio::spawn(async move {
            let serialized_entries = entries
                .into_iter()
                .map(|(key, value)| {
                    bincode::serialize(&DBEntry::HashMapEntry(id.clone(), key, value))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(|vecs| vecs.into_iter().flatten().collect::<Vec<u8>>())?;
            serialize_to_file(file, serialized_entries)?;
            return Ok(old_values);
        })
    }

    /// Gets a reference to the value corresponding to the given key.
    ///
    /// Returns None if the key does not exist.
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<ValueRef<'_, K, V>> {
        self.inner.get(key).map(|inner| ValueRef::new(inner))
    }

    /// Removes a key from the HashMap, returning the value at the key if the key was previously in the HashMap.
    ///
    /// Returns None if the key did not exist.
    pub fn remove(&self, key: &K) -> Option<JoinHandle<Result<Option<V>, StructureError>>> {
        if let Some((key, value)) = self.inner.remove(key) {
            let file = self.file.clone();
            let id = self.id.clone();
            Some(tokio::spawn(async move {
                let data =
                    bincode::serialize(&DBEntry::<K, V>::RemoveHashMapEntry(id.clone(), key))?;
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
        let id = self.id.clone();
        tokio::spawn(async move {
            let serialized_entries = keys
                .into_iter()
                .map(|key| {
                    bincode::serialize(&DBEntry::<K, V>::RemoveHashMapEntry(id.clone(), key))
                })
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
        drop(file);
        let mut cursor = std::io::Cursor::new(buffer);
        let mut entries_to_keep = Vec::new();
        while let Ok(entry) = bincode::deserialize_from::<_, DBEntry<K, V>>(&mut cursor) {
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

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use crate::db::DBMaker;

    use super::*;

    fn temp_file() -> Arc<Mutex<File>> {
        Arc::new(Mutex::new(tempfile::tempfile().unwrap()))
    }

    #[test]
    fn test_with_config() {
        let file = temp_file();
        let config = HashMapConfigBuilder::default()
            .shard_amount(8)
            .capacity(112)
            .build()
            .unwrap();
        let map = HashMap::<String, String>::with_config(file, vec![1], config).unwrap();
        assert_eq!(map.len(), 0);
        assert_eq!(map.is_empty(), true);
        assert_eq!(map.inner.capacity(), 112);
    }

    #[tokio::test]
    async fn test_insert_batch_and_get() {
        let file = temp_file();
        let map = HashMap::new(file, vec![2]).unwrap();
        let entries = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ];
        let _ = map.insert_batch(entries.clone()).await.unwrap().unwrap();
        for (key, value) in entries {
            assert_eq!(map.get(&key).unwrap().value(), &value);
        }
    }

    #[tokio::test]
    async fn test_remove_batch() {
        let file = temp_file();
        let map = HashMap::new(file, vec![3]).unwrap();
        let entries = vec![
            ("key3".to_string(), "value3".to_string()),
            ("key4".to_string(), "value4".to_string()),
        ];
        let _ = map.insert_batch(entries.clone()).await.unwrap().unwrap();
        let keys: Vec<_> = entries.iter().map(|(k, _)| k.clone()).collect();
        let _ = map.remove_batch(keys).await.unwrap().unwrap();
        for (key, _) in entries {
            assert!(map.get(&key).is_none());
        }
    }

    #[tokio::test]
    async fn test_clear() {
        let file = temp_file();
        let map = HashMap::new(file, vec![4]).unwrap();
        map.insert("key5".to_string(), "value5".to_string()).await.unwrap().unwrap();
        assert_eq!(map.is_empty(), false);
        map.clear().unwrap();
        assert_eq!(map.is_empty(), true);
    }

    #[tokio::test]
    async fn test_insert_existing_key() {
        let file = temp_file();
        let map = HashMap::new(file, vec![5]).unwrap();
        let key = "key6".to_string();
        let value1 = "value6".to_string();
        let value2 = "value7".to_string();

        // Insert key for the first time
        map.insert(key.clone(), value1.clone()).await.unwrap().unwrap();
        assert_eq!(map.get(&key).unwrap().value(), &value1);

        // Insert again with the same key but different value
        map.insert(key.clone(), value2.clone()).await.unwrap().unwrap();
        assert_eq!(map.get(&key).unwrap().value(), &value2);
    }

    #[tokio::test]
    async fn test_concurrent_inserts() {
        let file = temp_file();
        let map = Arc::new(HashMap::new(file, vec![6]).unwrap());

        let map1 = map.clone();
        let handle1 = tokio::spawn(async move {
            map1.insert("key7".to_string(), "value8".to_string()).await.unwrap()
        });

        let map2 = map.clone();
        let handle2 = tokio::spawn(async move {
            map2.insert("key8".to_string(), "value9".to_string()).await.unwrap()
        });

        let _ = handle1.await;
        let _ = handle2.await;

        assert_eq!(map.get(&"key7".to_string()).unwrap().value(), &"value8".to_string());
        assert_eq!(map.get(&"key8".to_string()).unwrap().value(), &"value9".to_string());
    }

    #[tokio::test]
    async fn test_error_handling_nonexistent_key() {
        let file = temp_file();
        let map = HashMap::<String, String>::new(file, vec![7]).unwrap();
        let result = map.remove(&"nonexistent_key".to_string());
        assert!(result.is_none());
    }



    #[tokio::test]
    async fn test_insert_and_get_serialization() {
        let map = create::<String, String>(
            "test_insert_and_get_serialization.db",
            "test_insert_and_get_serialization",
        );
        let key = "key".to_string();
        let value = "value".to_string();
        map.insert(key.clone(), value.clone()).await.unwrap().unwrap();
        drop(map);
        let map = create::<String, String>(
            "test_insert_and_get_serialization.db",
            "test_insert_and_get_serialization",
        );
        assert_eq!(map.get(&key).unwrap().value(), &value);
        std::fs::remove_file("test_insert_and_get_serialization.db").unwrap();
    }

    #[tokio::test]
    async fn test_insert_remove_serialization() {
        let map = create::<String, String>(
            "test_insert_remove_serialization.db",
            "test_insert_remove_serialization",
        );
        let key = "key".to_string();
        let value = "value".to_string();
        map.insert(key.clone(), value.clone()).await.unwrap().unwrap();
        assert_eq!(map.get(&key).unwrap().value(), &value);
        drop(map);
        let map = create::<String, String>(
            "test_insert_remove_serialization.db",
            "test_insert_remove_serialization",
        );
        assert_eq!(map.get(&key).unwrap().value(), &value);
        map.remove(&key).unwrap().await.unwrap().unwrap();
        assert!(map.get(&key).is_none());
        std::fs::remove_file("test_insert_remove_serialization.db").unwrap();
    }

    fn create<K, V>(filename: &str, id: &str) -> HashMap<K, V>
    where
        K: Hash + Eq + Serialize + for<'de> Deserialize<'de> + Clone + Send  + 'static,
        V: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
    {
        let db = DBMaker::file_db(PathBuf::from(filename)).make().unwrap();
        db.hash_map(id.to_string()).unwrap()
    }
}

