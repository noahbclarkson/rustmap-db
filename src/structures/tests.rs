#[cfg(test)]
mod hashmap_tests {

    use std::{
        fs::File,
        hash::Hash,
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use serde::{Deserialize, Serialize};

    use crate::{
        db::DBMaker,
        structures::{HashMap, HashMapConfigBuilder},
    };

    fn temp_file() -> Arc<Mutex<File>> {
        Arc::new(Mutex::new(tempfile::tempfile().unwrap()))
    }

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
        assert_eq!(map.inner.capacity(), 112);
    }

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

    #[tokio::test]
    async fn test_error_handling_nonexistent_key() {
        let file = temp_file();
        let map = HashMap::<String, String>::new(file, vec![7]).unwrap();
        let result = map.remove(&"nonexistent_key".to_string());
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_insert_and_get_serialization() {
        let map = create::<String, String>(
            "test_insert_and_get.db",
            "test_insert_and_get",
        );
        let key = "key".to_string();
        let value = "value".to_string();
        map.insert(key.clone(), value.clone())
            .await
            .unwrap()
            .unwrap();
        drop(map);
        let map = create::<String, String>(
            "test_insert_and_get.db",
            "test_insert_and_get",
        );

        assert_eq!(map.get(&key).unwrap().value(), &value);
        std::fs::remove_file("test_insert_and_get.db").unwrap();
    }

    #[tokio::test]
    async fn test_insert_remove_serialization() {
        let map = create::<String, String>(
            "test_insert_remove.db",
            "test_insert_remove",
        );
        let key = "key".to_string();
        let value = "value".to_string();
        map.insert(key.clone(), value.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(map.get(&key).unwrap().value(), &value);
        drop(map);
        let map = create::<String, String>(
            "test_insert_remove.db",
            "test_insert_remove",
        );
        assert_eq!(map.get(&key).unwrap().value(), &value);
        map.remove(&key).unwrap().await.unwrap().unwrap();
        assert!(map.get(&key).is_none());
        std::fs::remove_file("test_insert_remove.db").unwrap();
    }

    #[tokio::test]
    async fn test_clear_serialization() {
        let map = create::<String, String>(
            "test_clear.db",
            "test_clear",
        );
        let map2 = create::<String, String>(
            "test_clear.db",
            "test_clear_2",
        );
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
        let map = create::<String, String>(
            "test_clear.db",
            "test_clear",
        );
        let map2 = create::<String, String>(
            "test_clear.db",
            "test_clear_2",
        );
        assert!(map.get(&key).is_none());
        assert_eq!(map2.get(&key).unwrap().value(), &value);
        std::fs::remove_file("test_clear.db").unwrap();
    }

    fn create<K, V>(filename: &str, id: &str) -> HashMap<K, V>
    where
        K: Hash + Eq + Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
        V: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
    {
        let db = DBMaker::file_db(PathBuf::from(filename)).make().unwrap();
        db.hash_map(id.to_string()).unwrap()
    }
}
