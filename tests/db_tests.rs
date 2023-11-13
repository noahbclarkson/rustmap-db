use std::{fs::File, io::Read as _, path::PathBuf};

use rustmap_db::DBMaker;


#[tokio::test]
async fn test_hashmap_and_hashset_insert_serialization() {
    let filename = "test_hashmap_and_hashset.db";
    let db = DBMaker::file_db(PathBuf::from(filename)).make().unwrap();
    let hashmap = db.hash_map::<String, String>("test_hashmap".to_string()).unwrap();
    let hashset = db.hash_set::<String>("test_hashset".to_string()).unwrap();
    hashmap.insert("key".to_string(), "value".to_string()).await.unwrap().unwrap();
    hashset.insert("key".to_string()).await.unwrap().unwrap();
    // Print all the bytes in the file as u8
    let mut file = File::open(filename).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();
    drop(hashmap);
    drop(hashset);
    let db = DBMaker::file_db(PathBuf::from(filename)).make().unwrap();
    let hashmap = db.hash_map::<String, String>("test_hashmap".to_string()).unwrap();
    let hashset = db.hash_set::<String>("test_hashset".to_string()).unwrap();
    assert_eq!(hashmap.get(&"key".to_string()).unwrap().value(), &"value".to_string());
    assert!(hashset.get(&"key".to_string()).is_some());
    std::fs::remove_file(filename).unwrap();
}

