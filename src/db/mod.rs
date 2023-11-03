pub mod db_entry;

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::structures::structure_error::StructureError;
use crate::structures::{HashMap, HashMapConfig};

pub struct DBMaker {
    path: PathBuf,
}

impl DBMaker {
    /// Creates a new DB with a given path.
    pub fn file_db<P: Into<PathBuf>>(path: P) -> Self {
        Self { path: path.into() }
    }

    /// Creates the DB.
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

    /// Flushes the database to disk.
    pub fn flush(&self) -> io::Result<()> {
        self.file.lock().unwrap().flush()?;
        Ok(())
    }

    /// Creates a new HashMap with a capacity of 0.
    pub fn hash_map<
        K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Clone + Send + 'static,
        V: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
    >(
        &self,
    ) -> Result<HashMap<K, V>, StructureError> {
        Ok(HashMap::new(self.file.clone())?)
    }

    /// Creates a new HashMap with a given capacity and/or shard-amount.
    pub fn hash_map_with_config<
        K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Clone + Send + 'static,
        V: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
    >(
        &self,
        config: HashMapConfig,
    ) -> Result<HashMap<K, V>, StructureError> {
        Ok(HashMap::with_config(self.file.clone(), config)?)
    }
}
