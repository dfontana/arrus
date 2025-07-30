use std::io;

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("Failed to read /proc directory: {0}")]
    ProcReadError(#[from] io::Error),

    #[error("Failed to parse PID: {0}")]
    PidParseError(String),

    #[error("Failed to read cmdline for PID {pid}: {source}")]
    CmdlineReadError { pid: u32, source: io::Error },
}

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("Failed to load database from {path}: {source}")]
    LoadError {
        path: String,
        source: serde_json::Error,
    },

    #[error("Database file not found: {0}")]
    FileNotFound(String),

    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    #[error("Database manager error: {0}")]
    ManagerError(#[from] crate::db::DatabaseError),
}

#[derive(Debug, thiserror::Error)]
pub enum DetectorError {
    #[error("Process error: {0}")]
    ProcessError(#[from] ProcessError),

    #[error("Database error: {0}")]
    DatabaseError(#[from] DatabaseError),

    #[error("Send error: {0}")]
    SendError(String),
}
