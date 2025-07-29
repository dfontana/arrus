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
