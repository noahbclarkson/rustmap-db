//! Structures module for rustmap-db.
//!
//! This module provides the key-value storage structures with persistence capabilities.

use std::{
    fs::File,
    io::{Seek as _, SeekFrom, Write as _},
    sync::{Arc, Mutex},
};

use serde::Serialize;

use crate::StructureError;

pub mod hashmap;
pub mod structure_error;
pub mod value_ref;
pub mod hashset;

#[inline]
fn lock_file(file: &Arc<Mutex<File>>) -> Result<std::sync::MutexGuard<'_, File>, StructureError> {
    file.lock().map_err(|_| StructureError::MutexLockError)
}

#[inline]
fn serialize_to_file<T: Serialize>(data: &T, file: &Arc<Mutex<File>>) -> Result<(), StructureError> {
    let mut file = file.lock().map_err(|_| StructureError::MutexLockError)?;
    file.seek(SeekFrom::End(0))?;
    let serialized_data = bincode::serialize(data)?;
    file.write_all(&serialized_data)?;
    file.flush()?;
    Ok(())
}