#[derive(Debug, thiserror::Error)]
pub enum StructureError {
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("BinCode Error {0}")]
    BinCodeError(#[from] bincode::Error),
    #[error("Mutex Lock Error")]
    MutexLockError,
}
