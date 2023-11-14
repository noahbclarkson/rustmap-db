
# rustmap-db

![Rust Version](https://img.shields.io/badge/rust-2021-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)

## Overview
`rustmap-db` is an advanced, high-performance key-value store library implemented in Rust. It provides persistent, disk-backed storage solutions with an emphasis on concurrency and safety. This project leverages Rust's powerful type system and concurrency model to ensure thread-safe operations and efficient data management.

### Key Features
- **Concurrency-Friendly**: Utilizes `DashMap` for high-performance, concurrent access.
- **Asynchronous API**: Full support for non-blocking, asynchronous operations using Tokio.
- **Serialization/Deserialization**: Integrated with `Serde` for seamless data serialization.
- **Customizable HashMap Configuration**: Flexible API to tailor performance according to use case.
- **Comprehensive Benchmarks and Tests**: Includes extensive benchmarks and tests for reliability and performance tuning.

## Getting Started

To use `rustmap-db` in your project, add the following to your `Cargo.toml`:

```toml
[dependencies]
rustmap-db = { git = "https://github.com/noahbclarkson/rustmap-db.git", branch = "main" }
```

### Prerequisites
- Rust 2021 Edition
- Cargo package manager

### Installation
Clone the repository and build the project:

```bash
git clone https://github.com/noahbclarkson/rustmap-db.git
cd rustmap-db
cargo build --release
```

### Quick Example

```rust
use rustmap_db::DBMaker;

#[tokio::main]
async fn main() {
    let db = DBMaker::file_db("my_database.db").make().unwrap();
    let hashmap = db.hash_map::<String, String>("my_map").unwrap();

    hashmap.insert("key1".to_string(), "value1".to_string()).await.unwrap();
    println!("Value: {:?}", hashmap.get(&"key1".to_string()).await);
}
```

## Documentation
For detailed usage and API documentation, visit [rustmap-db documentation](https://docs.rs/rustmap-db).

## Contributing
Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for more information on how to get started.

## License
This project is licensed under the GNU General Public License v 3 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments
- DashMap for providing the underlying concurrent map.
- Tokio for the async runtime.
- Serde for serialization support.

---

Developed with :heart: by [noahbclarkson](https://github.com/noahbclarkson)
