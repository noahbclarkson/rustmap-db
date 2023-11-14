//! Error types for rustmap-db structures.
//!
//! This module defines the error types that can be encountered while
//! interacting with the `HashMap` and other data structures in `rustmap-db`.

use thiserror::Error;

/// An enumeration of all the error types that can occur in the `rustmap-db` data structures.
///
/// `StructureError` captures the various kinds of errors that can happen during
/// operations like insertion, removal, serialization, and deserialization of
/// data structure entries.
#[derive(Debug, Error)]
pub enum StructureError {
    /// An error that occurs during input/output operations, typically when
    /// reading from or writing to the backing file of the data structure.
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),

    /// An error that occurs during the serialization or deserialization process,
    /// indicating that something went wrong with the conversion to or from the
    /// binary format used for storage.
    #[error("Bincode Error {0}")]
    BinCodeError(#[from] bincode::Error),

    /// An error that occurs when a mutex lock could not be acquired. This typically
    /// indicates that another thread panicked while holding the lock or that the
    /// lock is somehow poisoned.
    #[error("Mutex Lock Error")]
    MutexLockError,
}
