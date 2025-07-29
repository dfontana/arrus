use thiserror::Error;

#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("Invalid port configuration: {0}")]
    InvalidPort(String),

    #[error("Failed to bind to address {0}: {1}")]
    BindFailed(String, std::io::Error),

    #[error("WebSocket error: {0}")]
    WebSocketError(#[from] axum::Error),

    #[error("JSON serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Broadcast channel closed")]
    BroadcastFailed,

    #[error("Client connection error: {0}")]
    ClientError(std::io::Error),
}

#[derive(Debug, Error)]
pub enum ArrusError {
    #[error("Socket {0} not found")]
    SocketNotFound(u32),

    #[error("Failed to send message")]
    SendError,

    #[error("Missing required arguments")]
    MissingArgs,

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Bridge error: {0}")]
    BridgeError(#[from] BridgeError),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}
