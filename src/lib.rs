use rand::Rng;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use crate::db::db::DBMaker;

pub mod db;
pub mod structures;

pub fn main() {
    let db = DBMaker::file_db("test.db").make().unwrap();
    let start = std::time::Instant::now();
    let cmap = db.hash_map::<String, i32>();
    println!("Creation took {:?}", start.elapsed());
    if cmap.is_err() {
        println!("{:?}", cmap.err().unwrap());
        return;
    }
    let cmap = cmap.unwrap();
    println!("Length: {}", cmap.len());
    let start = std::time::Instant::now();
    // Get 20,000 random keys from the map
    for i in 0..10000 {
        let key = format!("key{}", i);
        let value = cmap.get(&key);
        match value {
            Some(value) => assert_eq!(value, i),
            None => println!("Key {} not found", key),
        }
    }
    println!("Get took {:?}", start.elapsed());
    let start = std::time::Instant::now();
    // Add 10,000 random keys and values to the map
    for i in 0..10000 {
        let key = format!("key{}", i);
        let value = i;
        cmap.insert(key, value).unwrap();
    }
    println!("Insertion took {:?}", start.elapsed());
    let start = std::time::Instant::now();
    cmap.compact_db().unwrap();
    println!("Compaction took {:?}", start.elapsed());
    // let start = std::time::Instant::now();
    // // Insert 10,000 random keys and values to the map in a batch
    // let entries = (10000..20000)
    //     .into_par_iter()
    //     .map(|i| {
    //         let key = format!("key{}", i);
    //         let value = format!("value{}", i);
    //         (key, value)
    //     })
    //     .collect::<Vec<_>>();
    // cmap.insert_batch(entries).unwrap();
    // println!("Batch insertion took {:?}", start.elapsed());


}
