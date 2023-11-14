//! Test suite for the `HashSet` in rustmap-db.
//!
//! This module contains tests to validate the functionality of the `HashSet` data structure,
//! ensuring its correctness and reliability in various scenarios.

use rustmap_db::{DBMaker, HashSet};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tempfile::tempfile;

fn temp_file() -> Arc<Mutex<File>> {
    Arc::new(Mutex::new(tempfile().unwrap()))
}

#[tokio::test]
async fn test_insert_batch() {
    let file = temp_file();
    let hashset = HashSet::<String>::new(file, vec![4]).unwrap();
    let keys = vec!["key1".to_string(), "key2".to_string()];
    let _ = hashset.insert_batch(keys.clone()).await.unwrap();
    for key in keys {
        assert!(hashset.get(&key).is_some());
    }
}

/// Tests removing an element from the `HashSet`.
#[tokio::test]
async fn test_remove_element() {
    let file = temp_file();
    let hashset = HashSet::<String>::new(file, vec![5]).unwrap();
    let key = "key_to_remove".to_string();
    hashset.insert(key.clone()).await.unwrap().unwrap();
    let remove_result = hashset.remove(&key).unwrap().await.unwrap();
    assert_eq!(remove_result.unwrap(), Some(key.clone()));
    assert!(hashset.get(&key).is_none());
}

/// Tests the batch removal of elements.
#[tokio::test]
async fn test_remove_batch() {
    let file = temp_file();
    let hashset = HashSet::<String>::new(file, vec![6]).unwrap();
    let keys = vec!["key3".to_string(), "key4".to_string()];
    let _ = hashset.insert_batch(keys.clone()).await.unwrap();
    let _ = hashset.remove_batch(keys.clone()).await.unwrap();
    for key in keys {
        assert!(hashset.get(&key).is_none());
    }
}

/// Tests clearing the `HashSet`.
#[tokio::test]
async fn test_clear() {
    let file = temp_file();
    let hashset = HashSet::<String>::new(file, vec![7]).unwrap();
    hashset
        .insert("clear_key".to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hashset.is_empty(), false);
    hashset.clear().unwrap();
    assert_eq!(hashset.is_empty(), true);
}

/// Tests concurrent inserts to ensure thread safety.
#[tokio::test]
async fn test_concurrent_inserts() {
    let file = temp_file();
    let hashset = Arc::new(HashSet::<String>::new(file, vec![8]).unwrap());

    let hashset_clone1 = hashset.clone();
    let handle1 = tokio::spawn(async move {
        hashset_clone1
            .insert("concurrent_key1".to_string())
            .await
            .unwrap()
    });

    let hashset_clone2 = hashset.clone();
    let handle2 = tokio::spawn(async move {
        hashset_clone2
            .insert("concurrent_key2".to_string())
            .await
            .unwrap()
    });

    let _ = handle1.await;
    let _ = handle2.await;

    assert!(hashset.get(&"concurrent_key1".to_string()).is_some());
    assert!(hashset.get(&"concurrent_key2".to_string()).is_some());
}

/// Tests error handling with a non-existent key.
#[tokio::test]
async fn test_error_handling_nonexistent_key() {
    let file = temp_file();
    let hashset = HashSet::<String>::new(file, vec![9]).unwrap();
    let result = hashset.remove(&"nonexistent_key".to_string());
    assert!(result.is_none());
}

/// Tests serialization and deserialization during the insert and remove operations.
#[tokio::test]
async fn test_serialization() {
    let filename = "test_hashset_serialization.db";
    let hashset = create::<String, String>(filename, "test_serialization");

    let key = "serial_key".to_string();
    hashset.insert(key.clone()).await.unwrap().unwrap();
    drop(hashset);

    let db = DBMaker::file_db(PathBuf::from(filename)).make().unwrap();
    let hashset = db.hash_set("test_serialization".to_string()).unwrap();
    assert!(hashset.get(&key).is_some());

    hashset.remove(&key).unwrap().await.unwrap().unwrap();
    assert!(hashset.get(&key).is_none());

    std::fs::remove_file(filename).unwrap();
}

#[tokio::test]
async fn test_concurrent_insert_remove_hashset() {
    let file = temp_file();
    let hashset = Arc::new(HashSet::<String>::new(file, vec![11]).unwrap());
    let mut handles = vec![];

    // Spawning insert threads
    for i in 0..10 {
        let hashset_clone = hashset.clone();
        handles.push(tokio::spawn(async move {
            hashset_clone
                .insert(format!("element{}", i))
                .await
                .unwrap()
                .unwrap();
        }));
    }

    // Spawning remove threads
    for i in 0..10 {
        let hashset_clone = hashset.clone();
        handles.push(tokio::spawn(async move {
            hashset_clone
                .remove(&format!("element{}", i))
                .unwrap()
                .await
                .unwrap()
                .unwrap();
        }));
    }

    // Waiting for all threads to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verifying the final state of the HashSet
    for i in 0..10 {
        assert!(hashset.get(&format!("element{}", i)).is_none());
    }
}

/// Utility function to create a `HashSet` with a given id.
fn create<K, V>(filename: &str, id: &str) -> HashSet<K>
where
    K: Hash + Eq + Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static + std::fmt::Debug,
{
    let db = DBMaker::file_db(PathBuf::from(filename)).make().unwrap();
    db.hash_set(id.to_string()).unwrap()
}
