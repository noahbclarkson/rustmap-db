
use criterion::{criterion_group, criterion_main, Criterion};
use rustmap_db::{db::DBMaker, structures::HashMap};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
struct TestKey(u64);

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
struct TestValue(u64);

#[allow(dead_code)]
const ENTRIES: u64 = 500;

#[allow(dead_code)]
fn insert_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = DBMaker::file_db(PathBuf::from("insert-bench.db"))
        .make()
        .unwrap();
    let map: HashMap<TestKey, TestValue> = db.hash_map().unwrap();

    c.bench_function("insert", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut tasks = Vec::new();
                for i in 0..ENTRIES {
                    tasks.push(map.insert(TestKey(i), TestValue(i)));
                }
                futures::future::join_all(tasks).await;
            });
        })
    });

    // Remove the file
    std::fs::remove_file("insert-bench.db").unwrap();
}

#[allow(dead_code)]
fn batch_insert_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = DBMaker::file_db(PathBuf::from("insert-batch-bench.db"))
        .make()
        .unwrap();
    let map: HashMap<TestKey, TestValue> = db.hash_map().unwrap();

    c.bench_function("batch_insert", |b| {
        b.iter(|| {
            let entries: Vec<(TestKey, TestValue)> =
                (0..ENTRIES).map(|i| (TestKey(i), TestValue(i))).collect();
            rt.block_on(async {
                let result = map.insert_batch(entries);
                result.await.unwrap().unwrap();
            });
        })
    });

    // Remove the file
    std::fs::remove_file("insert-batch-bench.db").unwrap();
}

#[allow(dead_code)]
fn load_from_file_bench(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = DBMaker::file_db(PathBuf::from("load-from-file-bench.db"))
        .make()
        .unwrap();
    let map: HashMap<TestKey, TestValue> = db.hash_map().unwrap();

    // Insert entries
    rt.block_on(async {
        let mut tasks = Vec::new();
        for i in 0..ENTRIES {
            tasks.push(map.insert(TestKey(i), TestValue(i)));
        }
        futures::future::join_all(tasks).await;
    });

    c.bench_function("load_from_file", |b| {
        b.iter(|| {
            rt.block_on(async {
                let map: HashMap<TestKey, TestValue> = db.hash_map().unwrap();
                drop(map);
            });
        })
    });

    // Remove the file
    std::fs::remove_file("load-from-file-bench.db").unwrap();
}

criterion_group!(benches, insert_benchmark, batch_insert_benchmark, load_from_file_bench);
criterion_main!(benches);
