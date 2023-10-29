use dashmap::{DashMap, TryReserveError};
use serde::{Deserialize, Serialize};
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{Seek, SeekFrom, Write};
use wyhash::WyHash;

use super::hashmap_db_entry::HashMapDBEntry;
use super::map_error::MapError;
use crate::db::db::Database;

pub struct WyHasher {
    state: WyHash,
}

impl WyHasher {
    pub fn new() -> Self {
        WyHasher {
            state: WyHash::with_seed(0),
        }
    }

    pub fn with_seed(seed: u64) -> Self {
        WyHasher {
            state: WyHash::with_seed(seed),
        }
    }
}

impl Hasher for WyHasher {
    fn finish(&self) -> u64 {
        self.state.finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.state.write(bytes);
    }
}

#[derive(Default, Clone)]
pub struct WyBuildHasher(u64);

impl WyBuildHasher {
    pub fn new() -> Self {
        WyBuildHasher(0)
    }

    pub fn with_seed(seed: u64) -> Self {
        WyBuildHasher(seed)
    }
}

impl BuildHasher for WyBuildHasher {
    type Hasher = WyHasher;

    fn build_hasher(&self) -> Self::Hasher {
        WyHasher::with_seed(self.0)
    }
}

pub struct ConcurrentHashMap<K, V> {
    map: DashMap<K, V, WyBuildHasher>,
    db: Database,
}

impl<
        K: Serialize + for<'de> Deserialize<'de> + Eq + Hash,
        V: Serialize + for<'de> Deserialize<'de>,
    > ConcurrentHashMap<K, V>
{
    pub fn new(db: Database) -> Result<Self, MapError> {
        let map = DashMap::with_hasher(WyBuildHasher::new());
        let mut instance = Self { map, db };
        instance.load_from_file()?;
        Ok(instance)
    }

    pub fn with_seed(db: Database, seed: u64) -> Result<Self, MapError> {
        let map = DashMap::with_hasher(WyBuildHasher::with_seed(seed));
        let mut instance = Self { map, db };
        instance.load_from_file()?;
        Ok(instance)
    }

    pub fn with_shard_amount(db: Database, shard_amount: usize) -> Result<Self, MapError> {
        let map = DashMap::with_capacity_and_hasher_and_shard_amount(
            0,
            WyBuildHasher::new(),
            shard_amount,
        );

        let mut instance = Self { map, db };
        instance.load_from_file()?;
        Ok(instance)
    }

    fn load_from_file(&mut self) -> Result<(), MapError> {
        let mut file = self.db.file.lock().map_err(|_| MapError::MutexLockError)?;
        file.seek(SeekFrom::Start(0))?;

        while let Ok(entry) = bincode::deserialize_from::<_, HashMapDBEntry<K, V>>(&mut *file) {
            match entry {
                HashMapDBEntry::Insert(key, value) => {
                    self.map.insert(key, value);
                }
                HashMapDBEntry::Remove(key) => {
                    self.map.remove(&key);
                }
            }
        }

        Ok(())
    }

    /// Rewrite the data file, removing all tombstones/duplicates and compacting the file.
    pub fn compact_db(&self) -> Result<(), MapError>
    where
        K: Clone,
        V: Clone,
    {
        let mut new_file = tempfile::tempfile()?;
        for (key, value) in self.iter() {
            bincode::serialize_into(&mut new_file, &HashMapDBEntry::Insert(key, value))?;
        }

        new_file.flush()?;
        new_file.seek(SeekFrom::Start(0))?;
        let mut file = self.db.file.lock().map_err(|_| MapError::MutexLockError)?;
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;
        std::io::copy(&mut new_file, &mut *file)?;
        Ok(())
    }

    pub fn insert(&self, key: K, value: V) -> Result<(), MapError>
    where
        K: Clone,
        V: Clone,
    {
        self.map.insert(key.clone(), value.clone());
        let mut file = self.db.file.lock().map_err(|_| MapError::MutexLockError)?;
        file.seek(SeekFrom::End(0))?;
        bincode::serialize_into(&mut *file, &HashMapDBEntry::Insert(key, value))?;
        Ok(())
    }

    pub fn insert_batch(&self, entries: Vec<(K, V)>) -> Result<(), MapError>
    where
        K: Clone,
        V: Clone,
    {
        let mut file = self.db.file.lock().map_err(|_| MapError::MutexLockError)?;
        file.seek(SeekFrom::End(0))?;
        for (key, value) in entries.iter() {
            self.map.insert(key.clone(), value.clone());
            bincode::serialize_into(&mut *file, &HashMapDBEntry::Insert(key, value))?;
        }
        Ok(())
    }

    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.map.get(key).map(|v| v.clone())
    }

    pub fn remove(&self, key: &K) -> Result<Option<(K, V)>, MapError>
    where
        K: Clone,
    {
        match self.map.remove(key) {
            Some((_, value)) => {
                let mut file = self.db.file.lock().map_err(|_| MapError::MutexLockError)?;
                file.seek(SeekFrom::End(0)).map_err(MapError::IoError)?;
                let entry: HashMapDBEntry<&K, V> = HashMapDBEntry::Remove(key);
                bincode::serialize_into(&mut *file, &entry)?;
                Ok(Some((key.clone(), value)))
            }
            None => Ok(None),
        }
    }

    pub fn remove_batch(&self, keys: Vec<K>) -> Result<Option<Vec<(K, V)>>, MapError> {
        let mut file = self.db.file.lock().map_err(|_| MapError::MutexLockError)?;
        file.seek(SeekFrom::End(0))?;
        let mut removals: Vec<(K, V)> = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some((_, value)) = self.map.remove(&key) {
                let entry: HashMapDBEntry<&K, V> = HashMapDBEntry::Remove(&key);
                bincode::serialize_into(&mut *file, &entry)?;
                removals.push((key, value));
            }
        }
        match removals.len() {
            0 => Ok(None),
            _ => Ok(Some(removals)),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn clear(&self) -> Result<(), MapError> {
        self.map.clear();
        let file = self.db.file.lock().map_err(|_| MapError::MutexLockError)?;
        file.set_len(0)?;
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (K, V)> + '_
    where
        K: Clone,
        V: Clone,
    {
        self.map
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
    }

    pub fn keys(&self) -> impl Iterator<Item = K> + '_
    where
        K: Clone,
    {
        self.map.iter().map(|entry| entry.key().clone())
    }

    pub fn values(&self) -> impl Iterator<Item = V> + '_
    where
        V: Clone,
    {
        self.map.iter().map(|entry| entry.value().clone())
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn capacity(&self) -> usize {
        self.map.capacity()
    }

    pub fn shrink_to_fit(&self) {
        self.map.shrink_to_fit()
    }

    pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError> {
        self.map.try_reserve(additional)
    }
}
