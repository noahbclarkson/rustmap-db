use dashmap::DashSet;
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

use super::{lock_file, serialize_to_file, value_ref::ValueRef};

/// Configuration for creating a `HashSet`.
///
/// Defines the parameters for creating a `HashSet`, including the initial capacity.
#[derive(Debug, Builder)]
pub struct HashSetConfig {
    #[builder(default = "0")]
    pub capacity: usize,
}

/// A file-backed, thread-safe hash set structure.
///
/// Provides a persistent, concurrent store for unique elements that are backed by a file.
/// Supports operations like `insert`, `get`, and `remove`, with changes being
/// written to disk for persistence.
#[derive(Debug)]
pub struct HashSet<K: Hash + Eq> {
    inner: DashSet<K>,
    file: Arc<Mutex<File>>,
    id: Vec<u8>,
}

impl<K: Hash + Eq> HashSet<K>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static + std::fmt::Debug,
{
    /// Creates a new `HashSet` with the default capacity.
    ///
    /// Initializes an empty `HashSet` with default settings and loads existing data from the file if available.
    pub fn new(file: Arc<Mutex<File>>, id: Vec<u8>) -> Result<Self, StructureError> {
        let instance = Self {
            inner: DashSet::new(),
            file,
            id,
        };
        instance.load_from_file()?;
        Ok(instance)
    }

    /// Creates a new `HashSet` with specified configuration.
    ///
    /// Allows for custom configuration of the `HashSet`, including setting the initial capacity.
    pub fn with_config(
        file: Arc<Mutex<File>>,
        id: Vec<u8>,
        config: HashSetConfig,
    ) -> Result<Self, StructureError> {
        let instance = Self {
            inner: DashSet::with_capacity(config.capacity),
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
                    DBEntry::HashSetEntry(id, key) => {
                        if id == self.id {
                            let key = bincode::deserialize::<K>(&key)?;
                            self.inner.insert(key);
                        }
                    }
                    DBEntry::RemoveHashSetEntry(id, key) => {
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

    /// Inserts a batch of elements into the `HashSet`.
    ///
    /// More efficient than individual `insert` calls for adding multiple elements. Returns a `JoinHandle` to await the operation's completion.
    #[inline]
    pub fn insert(&self, key: K) -> JoinHandle<Result<bool, StructureError>> {
        let old_value = self.inner.insert(key.clone());
        let file = self.file.clone();
        let id = self.id.clone();
        tokio::spawn(async move {
            let key = bincode::serialize(&key)?;
            serialize_to_file(&DBEntry::HashSetEntry(id.clone(), key), &file)?;
            Ok(old_value)
        })
    }

    /// Inserts a batch of elements into the `HashSet`.
    ///
    /// More efficient than individual `insert` calls for adding multiple elements. Returns a `JoinHandle` to await the operation's completion.
    pub fn insert_batch(&self, entries: Vec<K>) -> JoinHandle<Result<Vec<bool>, StructureError>> {
        let mut old_values = Vec::with_capacity(entries.len());
        for key in &entries {
            old_values.push(self.inner.insert(key.clone()));
        }

        let file = self.file.clone();
        let id = self.id.clone();
        tokio::spawn(async move {
            let entries = entries
                .into_iter()
                .map(|key| {
                    let key = bincode::serialize(&key)?;
                    Ok(DBEntry::HashSetEntry(id.clone(), key))
                })
                .collect::<Result<Vec<DBEntry>, StructureError>>()?;
            serialize_to_file(&entries, &file)?;
            return Ok(old_values);
        })
    }

    /// Retrieves a reference to the element, if present in the `HashSet`.
    ///
    /// Returns `None` if the element is not found.
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<ValueRef<'_, K>> {
        self.inner.get(key).map(|inner| ValueRef::new(inner))
    }

    /// Removes an element from the `HashSet`, returning it if it was present.
    ///
    /// Returns a `JoinHandle` that can be awaited to determine the result of the operation.
    pub fn remove(&self, key: &K) -> Option<JoinHandle<Result<Option<K>, StructureError>>> {
        if let Some(key) = self.inner.remove(key) {
            let file = self.file.clone();
            let id = self.id.clone();
            Some(tokio::spawn(async move {
                let k = bincode::serialize(&key)?;
                serialize_to_file(&DBEntry::RemoveHashSetEntry(id.clone(), k), &file)?;
                Ok(Some(key))
            }))
        } else {
            None
        }
    }

    /// Removes a batch of elements from the `HashSet`.
    ///
    /// More efficient than individual `remove` calls for removing multiple elements. Returns a `JoinHandle` to await the operation's completion.
    pub fn remove_batch(&self, keys: Vec<K>) -> JoinHandle<Result<Vec<K>, StructureError>> {
        let mut removed_values = Vec::with_capacity(keys.len());
        for key in &keys {
            if let Some(key) = self.inner.remove(key) {
                removed_values.push(key);
            }
        }

        let file = self.file.clone();
        let id = self.id.clone();
        tokio::spawn(async move {
            let entries = removed_values
                .clone()
                .into_iter()
                .map(|key| {
                    let key = bincode::serialize(&key)?;
                    Ok(DBEntry::RemoveHashMapEntry(id.clone(), key))
                })
                .collect::<Result<Vec<DBEntry>, StructureError>>()?;
            serialize_to_file(&entries, &file)?;
            Ok(removed_values)
        })
    }

    /// Returns the number of elements in the `HashSet`.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Checks if the `HashSet` is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Clears all elements from the `HashSet`.
    ///
    /// This operation is thread-safe and ensures changes are persisted to disk.
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
                DBEntry::HashSetEntry(id, _) => {
                    if id != self.id {
                        entries_to_keep.push(entry);
                    }
                }
                DBEntry::RemoveHashSetEntry(id, _) => {
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

    /// Returns the capacity of the `HashSet`.
    ///
    /// The capacity is the number of elements the `HashSet` can hold without reallocating memory.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}
