//! Database module for rustmap-db.
//!
//! This module provides the foundational database structures and functionality
//! for creating and interacting with the database file. It includes the definition
//! and implementation of the `DBMaker` struct for configuring and creating new
//! `Database` instances, as well as the `Database` struct itself which serves as
//! a wrapper around a file, providing methods to interact with the underlying
//! database entries. This module is integral for managing the storage, retrieval,
//! and manipulation of data in a persistent manner.

pub mod db_entry;

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::{HashMap, HashMapConfig, StructureError};

/// A builder for creating a new `Database` instance.
///
/// `DBMaker` is a structure that configures and initializes a new `Database`. It is designed
/// to provide a fluent interface for database creation with customization options for the file path.
/// The `DBMaker` is responsible for handling the initialization of a `Database` including the
/// opening or creation of the database file.
pub struct DBMaker {
    path: PathBuf,
}

impl DBMaker {
    /// Creates a new `DBMaker` for a file-based database at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - A `PathBuf` that points to the desired database file location.
    pub fn file_db<P: Into<PathBuf>>(path: P) -> Self {
        Self { path: path.into() }
    }

    /// Consumes the `DBMaker`, attempting to create a `Database`.
    ///
    /// This function attempts to open or create the database file at the specified path,
    /// returning a `Database` instance on success. It encapsulates the logic required for
    /// the initialization of a `Database`, handling the creation or opening of the database file.
    pub fn make(self) -> io::Result<Database> {
        Database::open(self.path)
    }
}

/// The main database structure for rustmap-db.
///
/// `Database` is a wrapper around a file that provides methods to interact with
/// the underlying database entries. It handles the serialization and deserialization
/// of data to and from the database file, encapsulating the file I/O logic required
/// for persistent storage. This struct is central to the `rustmap-db` library, as it
/// provides the mechanisms for reading from and writing to the database.
#[derive(Clone)]
pub struct Database {
    pub(crate) file: Arc<Mutex<File>>,
}

impl Database {
    /// Opens the database file at the given path and returns a `Database` instance.
    ///
    /// This method is responsible for initializing a `Database` instance by opening the
    /// specified database file. It sets up the file with read and write capabilities, and
    /// wraps it in an `Arc<Mutex<_>>` to allow for concurrent access.
    ///
    /// # Arguments
    ///
    /// * `path` - A `PathBuf` that points to the database file.
    ///
    /// # Errors
    ///
    /// Will return an `io::Error` if the file cannot be created or opened.
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
    ///
    /// This method ensures that all buffered writes to the database file are committed
    /// to disk. It is crucial for maintaining data integrity, especially after a series
    /// of write operations.
    pub fn flush(&self) -> io::Result<()> {
        self.file.lock().unwrap().flush()?;
        Ok(())
    }

    /// Creates a new HashMap with a capacity of 0.
    ///
    /// This method facilitates the creation of a new `HashMap` instance linked to the database,
    /// with a default capacity of 0. It is a convenience function for quickly initializing
    /// a hash map without custom configurations.
    ///
    /// # Arguments
    ///
    /// * `id` - A `String` identifier for the hashmap, unique within the database.
    ///
    /// # Errors
    ///
    /// Returns `StructureError` if there is an issue in the creation process.
    pub fn hash_map<
        K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Clone + Send + 'static,
        V: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
    >(
        &self,
        id: String,
    ) -> Result<HashMap<K, V>, StructureError> {
        Ok(HashMap::new(self.file.clone(), to_raw_id(id))?)
    }

    /// Creates a new HashMap with a given capacity and/or shard-amount.
    ///
    /// This method allows for the creation of a `HashMap` with specific configurations
    /// such as capacity and shard amount. It is intended for situations where fine-tuning
    /// of the hashmap's properties is required for performance or specific use cases.
    ///
    /// # Arguments
    ///
    /// * `id` - A `String` identifier for the hashmap, unique within the database.
    /// * `config` - The configuration for the hashmap, including capacity and shard amount.
    ///
    /// # Errors
    ///
    /// Returns `StructureError` if there is an issue in the creation process.
    pub fn hash_map_with_config<
        K: Serialize + for<'de> Deserialize<'de> + Eq + Hash + Clone + Send + 'static,
        V: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
    >(
        &self,
        id: String,
        config: HashMapConfig,
    ) -> Result<HashMap<K, V>, StructureError> {
        Ok(HashMap::with_config(
            self.file.clone(),
            to_raw_id(id),
            config,
        )?)
    }
}

/// Converts a string identifier to a raw byte representation.
///
/// This utility function is used to transform a string-based identifier into a byte array
/// that can be used for uniquely identifying data structures like hash maps within the database.
///
/// # Arguments
///
/// * `id` - A `String` that serves as the identifier.
///
/// # Returns
///
/// Returns a `Vec<u8>` that represents the raw byte format of the identifier.
pub(crate) fn to_raw_id(id: String) -> Vec<u8> {
    let mut raw_id = Vec::new();
    raw_id.extend_from_slice(&id.len().to_be_bytes());
    raw_id.extend_from_slice(id.as_bytes());
    raw_id
}