use std::io;

#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("Failed to read /proc directory: {0}")]
    ProcRead(#[from] io::Error),

    #[error("Failed to parse PID: {0}")]
    PidParse(String),

    #[error("Failed to read cmdline for PID {pid}: {source}")]
    CmdlineRead { pid: u32, source: io::Error },
}

#[allow(dead_code)]
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

#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub enum DetectorError {
    #[error("Process error: {0}")]
    Process(#[from] ProcessError),

    #[error("Database error: {0}")]
    Database(#[from] DatabaseError),

    #[error("Send error: {0}")]
    Send(String),
}
