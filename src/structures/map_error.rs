#[derive(Debug, thiserror::Error)]
pub enum MapError {
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Bincode Error {0}")]
    BincodeError(#[from] bincode::Error),
    #[error("Mutex Lock Error")]
    MutexLockError,
}

