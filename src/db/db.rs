use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::structures::concurrenthashmap::ConcurrentHashMap;
use crate::structures::map_error::MapError;

pub struct DBMaker {
    path: PathBuf,
}

impl DBMaker {
    pub fn file_db<P: Into<PathBuf>>(path: P) -> Self {
        Self { path: path.into() }
    }

    pub fn make(self) -> io::Result<Database> {
        Database::open(self.path)
    }
}

#[derive(Clone)]
pub struct Database {
    pub file: Arc<Mutex<File>>,
}

impl Database {
    fn open(path: PathBuf) -> io::Result<Self> {
        let file = Arc::new(Mutex::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?,
        ));
        Ok(Self { file })
    }

    pub fn hash_map<
        K: Serialize + for<'de> Deserialize<'de> + Eq + Hash,
        V: Serialize + for<'de> Deserialize<'de>,
    >(
        &self,
    ) -> Result<ConcurrentHashMap<K, V>, MapError> {
        Ok(ConcurrentHashMap::new(self.clone())?)
    }

    pub fn hash_map_from_seed<
        K: Serialize + for<'de> Deserialize<'de> + Eq + Hash,
        V: Serialize + for<'de> Deserialize<'de>,
    >(
        &self,
        seed: u64,
    ) -> Result<ConcurrentHashMap<K, V>, MapError> {
        Ok(ConcurrentHashMap::with_seed(self.clone(), seed)?)
    }

    pub fn hash_map_with_shard_amount<
        K: Serialize + for<'de> Deserialize<'de> + Eq + Hash,
        V: Serialize + for<'de> Deserialize<'de>,
    >(
        &self,
        shard_amount: usize,
    ) -> Result<ConcurrentHashMap<K, V>, MapError> {
        Ok(ConcurrentHashMap::with_shard_amount(
            self.clone(),
            shard_amount,
        )?)
    }

    pub fn close(&self) -> io::Result<()> {
        self.file.lock().unwrap().flush()?;
        Ok(())
    }
}
