//! Test suite for the rustmap-db structures.
//!
//! This module contains unit and integration tests that validate the functionality
//! of the `rustmap-db` data structures, ensuring correctness and reliability.

#[cfg(test)]
use std::{
    fs::File,
    hash::Hash,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use rustmap_db::{DBMaker, HashMap, HashMapConfigBuilder};
use serde::{Deserialize, Serialize};

// Below are the tests for the HashMap structure.
// Each test function is documented to describe its purpose and the specific functionality it is verifying.

fn temp_file() -> Arc<Mutex<File>> {
    Arc::new(Mutex::new(tempfile::tempfile().unwrap()))
}

/// Tests the creation of a `HashMap` with a specified configuration.
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
    assert_eq!(map.capacity(), 112);
}

/// Tests the batch insertion and retrieval of key-value pairs.
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

/// Tests the batch removal of keys.
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

/// Tests clearing the `HashMap`.
#[tokio::test]
async fn test_clear() {
    let file = temp_file();
    let map = HashMap::new(file, vec![4]).unwrap();
    map.insert("key5".to_string(), "value5".to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(map.is_empty(), false);
    map.clear().unwrap();
    assert_eq!(map.is_empty(), true);
}

/// Tests the insertion of an existing key to verify that the value is updated.
#[tokio::test]
async fn test_insert_existing_key() {
    let file = temp_file();
    let map = HashMap::new(file, vec![5]).unwrap();
    let key = "key6".to_string();
    let value1 = "value6".to_string();
    let value2 = "value7".to_string();

    // Insert key for the first time
    map.insert(key.clone(), value1.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(map.get(&key).unwrap().value(), &value1);

    // Insert again with the same key but different value
    map.insert(key.clone(), value2.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(map.get(&key).unwrap().value(), &value2);
}

/// Tests concurrent inserts to ensure thread safety.
#[tokio::test]
async fn test_concurrent_inserts() {
    let file = temp_file();
    let map = Arc::new(HashMap::new(file, vec![6]).unwrap());

    let map1 = map.clone();
    let handle1 = tokio::spawn(async move {
        map1.insert("key7".to_string(), "value8".to_string())
            .await
            .unwrap()
    });

    let map2 = map.clone();
    let handle2 = tokio::spawn(async move {
        map2.insert("key8".to_string(), "value9".to_string())
            .await
            .unwrap()
    });

    let _ = handle1.await;
    let _ = handle2.await;

    assert_eq!(
        map.get(&"key7".to_string()).unwrap().value(),
        &"value8".to_string()
    );
    assert_eq!(
        map.get(&"key8".to_string()).unwrap().value(),
        &"value9".to_string()
    );
}

/// Tests error handling with a non-existent key.
#[tokio::test]
async fn test_error_handling_nonexistent_key() {
    let file = temp_file();
    let map = HashMap::<String, String>::new(file, vec![7]).unwrap();
    let result = map.remove(&"nonexistent_key".to_string());
    assert!(result.is_none());
}

/// Tests serialization and deserialization during the insert operation.
#[tokio::test]
async fn test_insert_and_get_serialization() {
    let map = create::<String, String>("test_insert_and_get.db", "test_insert_and_get");
    let key = "key".to_string();
    let value = "value".to_string();
    map.insert(key.clone(), value.clone())
        .await
        .unwrap()
        .unwrap();
    drop(map);
    let map = create::<String, String>("test_insert_and_get.db", "test_insert_and_get");

    assert_eq!(map.get(&key).unwrap().value(), &value);
    std::fs::remove_file("test_insert_and_get.db").unwrap();
}

/// Tests serialization and deserialization when removing keys.
#[tokio::test]
async fn test_insert_remove_serialization() {
    let map = create::<String, String>("test_insert_remove.db", "test_insert_remove");
    let key = "key".to_string();
    let value = "value".to_string();
    map.insert(key.clone(), value.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(map.get(&key).unwrap().value(), &value);
    drop(map);
    let map = create::<String, String>("test_insert_remove.db", "test_insert_remove");
    assert_eq!(map.get(&key).unwrap().value(), &value);
    map.remove(&key).unwrap().await.unwrap().unwrap();
    assert!(map.get(&key).is_none());
    std::fs::remove_file("test_insert_remove.db").unwrap();
}

/// Tests that clearing a `HashMap` persists correctly to disk.
#[tokio::test]
async fn test_clear_serialization() {
    let map = create::<String, String>("test_clear.db", "test_clear");
    let map2 = create::<String, String>("test_clear.db", "test_clear_2");
    let key = "key".to_string();
    let value = "value".to_string();
    map.insert(key.clone(), value.clone())
        .await
        .unwrap()
        .unwrap();
    map2.insert(key.clone(), value.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(map.get(&key).unwrap().value(), &value);
    assert_eq!(map2.get(&key).unwrap().value(), &value);
    map.clear().unwrap();
    drop(map);
    drop(map2);
    let map = create::<String, String>("test_clear.db", "test_clear");
    let map2 = create::<String, String>("test_clear.db", "test_clear_2");
    assert!(map.get(&key).is_none());
    assert_eq!(map2.get(&key).unwrap().value(), &value);
    std::fs::remove_file("test_clear.db").unwrap();
}

#[tokio::test]
async fn test_concurrent_read_write() {
    let file = temp_file();
    let map = Arc::new(HashMap::new(file, vec![10]).unwrap());
    let mut handles = vec![];

    // Spawning write threads
    for i in 0..10 {
        let map_clone = map.clone();
        handles.push(tokio::spawn(async move {
            map_clone
                .insert(format!("key{}", i), format!("value{}", i))
                .await
                .unwrap()
                .unwrap();
        }));
    }

    // Spawning read threads
    for i in 0..10 {
        let map_clone = map.clone();
        handles.push(tokio::spawn(async move {
            assert!(map_clone.get(&format!("key{}", i)).is_some());
        }));
    }

    // Waiting for all threads to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verifying the entries
    for i in 0..10 {
        assert_eq!(
            map.get(&format!("key{}", i)).unwrap().value(),
            &format!("value{}", i)
        );
    }
}

/// Utility function to create a `HashMap` with a given id.
fn create<K, V>(filename: &str, id: &str) -> HashMap<K, V>
where
    K: Hash + Eq + Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
    V: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
{
    let db = DBMaker::file_db(PathBuf::from(filename)).make().unwrap();
    db.hash_map(id.to_string()).unwrap()
}
