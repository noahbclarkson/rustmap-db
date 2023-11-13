//! # Rustmap-DB
//!
//! `rustmap-db` is a Rust library for creating a persistent, disk-backed map structure. It offers
//! thread-safe access to a key-value store and includes various utility functions for effective data management.
//! This library is designed for scenarios where both performance and data persistence are crucial.

/// Database modules, containing core functionality for database operations.
///
/// The `db` module includes `DBMaker` for constructing new database instances
/// and `Database`, which encapsulates file handling logic for the database.
pub mod db;

/// Structures module, containing the key-value map implementation and related structures.
///
/// This module includes `HashMap`, a file-backed, concurrent map implementation,
/// and `StructureError`, an enum for error handling within map operations.
pub mod structures;

// Publicly re-export key components for easy access by library users.
pub use db::{DBMaker, Database};
pub use structures::{
    hashmap::{HashMap, HashMapConfig, HashMapConfigBuilder},
    hashset::{HashSet, HashSetConfig, HashSetConfigBuilder},
    structure_error::StructureError,
    value_ref::{ValueRef, ValueRefPair},
};
