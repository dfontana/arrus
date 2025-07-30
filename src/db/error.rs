use thiserror::Error;

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),

    #[error("HTTP error: status {status}, message: {message}")]
    HttpError { status: u16, message: String },

    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("File system error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Validation failed: {0}")]
    ValidationError(String),

    #[error("Backup operation failed: {0}")]
    BackupError(String),

    #[error("Atomic update verification failed")]
    VerificationFailed,

    #[error("Atomic update failed")]
    AtomicUpdateFailed,

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Scheduler error: {0}")]
    SchedulerError(String),
}

pub type Result<T> = std::result::Result<T, DatabaseError>;
