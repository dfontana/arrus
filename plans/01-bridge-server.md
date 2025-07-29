# Bridge Server Implementation Plan - Rust

## Overview

The Bridge Server is a WebSocket server component that forwards Rich Presence activity data from the main arRPC server to web clients (Discord web app). It acts as a message relay, maintaining a cache of the last activity for each socket and broadcasting updates to all connected web clients.

## Architecture

### High-Level Data Flow

```
RPC Server (SET_ACTIVITY) 
    ↓ 
Main Server EventEmitter ('activity' event)
    ↓
Bridge Server (receive via channel/callback)
    ↓
WebSocket Broadcast (JSON messages)
    ↓
Connected Web Clients (Discord web app)
```

### Core Components

1. **WebSocket Server** - Listens on configurable port (default 1337)
2. **Message Cache** - Stores last activity per socket ID
3. **Client Manager** - Tracks connected web clients
4. **Message Broadcaster** - Forwards activity data to all clients
5. **Configuration Manager** - Handles port configuration via environment

## Data Structures

### Activity Message Structure

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityMessage {
    pub socket_id: String,
    pub activity: Option<ActivityData>,
    pub pid: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityData {
    pub application_id: String,
    pub name: String,
    pub details: Option<String>,
    pub state: Option<String>,
    pub timestamps: Option<ActivityTimestamps>,
    pub assets: Option<ActivityAssets>,
    pub party: Option<ActivityParty>,
    pub secrets: Option<ActivitySecrets>,
    pub instance: Option<bool>,
    pub flags: Option<u32>,
    pub buttons: Option<Vec<String>>,
    pub metadata: Option<ActivityMetadata>,
    #[serde(rename = "type")]
    pub activity_type: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityTimestamps {
    pub start: Option<u64>,
    pub end: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityAssets {
    pub large_image: Option<String>,
    pub large_text: Option<String>,
    pub small_image: Option<String>,
    pub small_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityParty {
    pub id: Option<String>,
    pub size: Option<[u32; 2]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivitySecrets {
    pub join: Option<String>,
    pub spectate: Option<String>,
    #[serde(rename = "match")]
    pub match_secret: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityMetadata {
    pub button_urls: Option<Vec<String>>,
}
```

### Bridge Server State

```rust
pub struct BridgeServer {
    /// WebSocket server instance
    websocket_server: Option<tokio::task::JoinHandle<()>>,
    /// Cache of last messages per socket ID
    last_messages: Arc<RwLock<HashMap<String, ActivityMessage>>>,
    /// Set of connected web clients
    connected_clients: Arc<RwLock<HashSet<SocketAddr>>>,
    /// Channel sender for broadcasting messages
    broadcast_tx: mpsc::UnboundedSender<ActivityMessage>,
    /// Server configuration
    config: BridgeConfig,
    /// Logging instance
    logger: Logger,
}

#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub port: u16,
    pub bind_address: String,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            port: 1337,
            bind_address: "127.0.0.1".to_string(),
        }
    }
}
```

## Implementation Details

### 1. Configuration Management

```rust
impl BridgeConfig {
    pub fn from_env() -> Result<Self, BridgeError> {
        let mut config = Self::default();
        
        if let Ok(port_str) = std::env::var("ARRPC_BRIDGE_PORT") {
            config.port = port_str.parse()
                .map_err(|_| BridgeError::InvalidPort(port_str))?;
        }
        
        Ok(config)
    }
}
```

### 2. WebSocket Server Setup

**Recommended Implementation**: Consider using **Axum** with WebSocket support for a more robust and feature-rich WebSocket server implementation. Axum provides excellent WebSocket handling with built-in middleware support, structured routing, and better error handling.

```rust
// Alternative Axum-based implementation (recommended)
use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::Response,
    routing::get,
    Router,
};

// Fallback tokio-tungstenite implementation
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tokio::net::{TcpListener, TcpStream};

impl BridgeServer {
    pub async fn start(&mut self) -> Result<(), BridgeError> {
        let addr = format!("{}:{}", self.config.bind_address, self.config.port);
        let listener = TcpListener::bind(&addr).await
            .map_err(|e| BridgeError::BindFailed(addr.clone(), e))?;
        
        self.logger.info(&format!("Bridge server listening on {}", addr));
        
        let last_messages = Arc::clone(&self.last_messages);
        let connected_clients = Arc::clone(&self.connected_clients);
        let mut broadcast_rx = self.create_broadcast_receiver();
        let logger = self.logger.clone();
        
        // Spawn WebSocket server task
        let server_handle = tokio::spawn(async move {
            Self::run_websocket_server(
                listener,
                last_messages,
                connected_clients,
                broadcast_rx,
                logger
            ).await
        });
        
        self.websocket_server = Some(server_handle);
        Ok(())
    }
    
    async fn run_websocket_server(
        listener: TcpListener,
        last_messages: Arc<RwLock<HashMap<String, ActivityMessage>>>,
        connected_clients: Arc<RwLock<HashSet<SocketAddr>>>,
        mut broadcast_rx: mpsc::UnboundedReceiver<ActivityMessage>,
        logger: Logger,
    ) {
        loop {
            tokio::select! {
                // Handle new connections
                Ok((stream, addr)) = listener.accept() => {
                    let last_messages = Arc::clone(&last_messages);
                    let connected_clients = Arc::clone(&connected_clients);
                    let logger = logger.clone();
                    
                    tokio::spawn(async move {
                        Self::handle_client_connection(
                            stream,
                            addr,
                            last_messages,
                            connected_clients,
                            logger
                        ).await;
                    });
                }
                
                // Handle broadcast messages
                Some(message) = broadcast_rx.recv() => {
                    Self::broadcast_to_clients(&connected_clients, &message, &logger).await;
                }
            }
        }
    }
}
```

### 3. Client Connection Handling

```rust
impl BridgeServer {
    async fn handle_client_connection(
        stream: TcpStream,
        addr: SocketAddr,
        last_messages: Arc<RwLock<HashMap<String, ActivityMessage>>>,
        connected_clients: Arc<RwLock<HashSet<SocketAddr>>>,
        logger: Logger,
    ) {
        logger.info(&format!("Web client connected: {}", addr));
        
        // Add client to connected set
        {
            let mut clients = connected_clients.write().await;
            clients.insert(addr);
        }
        
        // Upgrade to WebSocket
        let ws_stream = match accept_async(stream).await {
            Ok(ws) => ws,
            Err(e) => {
                logger.error(&format!("WebSocket upgrade failed for {}: {}", addr, e));
                return;
            }
        };
        
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        
        // Send catch-up messages for newly connected client
        {
            let messages = last_messages.read().await;
            for (_, message) in messages.iter() {
                if message.activity.is_some() {
                    if let Ok(json) = serde_json::to_string(message) {
                        if let Err(e) = ws_sender.send(Message::Text(json)).await {
                            logger.error(&format!("Failed to send catch-up message to {}: {}", addr, e));
                            break;
                        }
                    }
                }
            }
        }
        
        // Handle client disconnect
        while let Some(msg) = ws_receiver.next().await {
            match msg {
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(Message::Ping(data)) => {
                    if ws_sender.send(Message::Pong(data)).await.is_err() {
                        break;
                    }
                }
                _ => {} // Ignore other message types for now
            }
        }
        
        // Remove client from connected set
        {
            let mut clients = connected_clients.write().await;
            clients.remove(&addr);
        }
        
        logger.info(&format!("Web client disconnected: {}", addr));
    }
}
```

### 4. Message Broadcasting

```rust
impl BridgeServer {
    pub async fn send_activity(&self, message: ActivityMessage) -> Result<(), BridgeError> {
        // Update cache
        {
            let mut cache = self.last_messages.write().await;
            cache.insert(message.socket_id.clone(), message.clone());
        }
        
        // Broadcast to all connected clients
        self.broadcast_tx.send(message)
            .map_err(|_| BridgeError::BroadcastFailed)?;
        
        Ok(())
    }
    
    async fn broadcast_to_clients(
        connected_clients: &Arc<RwLock<HashSet<SocketAddr>>>,
        message: &ActivityMessage,
        logger: &Logger,
    ) {
        let json = match serde_json::to_string(message) {
            Ok(json) => json,
            Err(e) => {
                logger.error(&format!("Failed to serialize message: {}", e));
                return;
            }
        };
        
        let clients = connected_clients.read().await;
        logger.debug(&format!("Broadcasting to {} clients", clients.len()));
        
        // Note: This is a simplified version. In practice, you'd need to maintain
        // individual WebSocket senders for each client, possibly in a separate
        // data structure alongside the connected_clients set.
    }
}
```

### 5. Integration with Main RPC Server

```rust
use tokio::sync::mpsc;

pub struct RpcBridgeIntegration {
    bridge_tx: mpsc::UnboundedSender<ActivityMessage>,
}

impl RpcBridgeIntegration {
    pub fn new(bridge_server: &BridgeServer) -> Self {
        Self {
            bridge_tx: bridge_server.get_sender(),
        }
    }
    
    pub fn on_activity_event(&self, activity_data: ActivityMessage) {
        if let Err(e) = self.bridge_tx.send(activity_data) {
            eprintln!("Failed to send activity to bridge: {}", e);
        }
    }
}
```

## Error Handling

### Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("Invalid port configuration: {0}")]
    InvalidPort(String),
    
    #[error("Failed to bind to address {0}: {1}")]
    BindFailed(String, std::io::Error),
    
    #[error("WebSocket error: {0}")]
    WebSocketError(#[from] tokio_tungstenite::tungstenite::Error),
    
    #[error("JSON serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    
    #[error("Broadcast channel closed")]
    BroadcastFailed,
    
    #[error("Client connection error: {0}")]
    ClientError(std::io::Error),
}
```

### Error Recovery Strategies

1. **Connection Failures**: Log error and continue accepting new connections
2. **Serialization Errors**: Log warning and skip malformed messages
3. **Client Disconnects**: Clean up resources and remove from client list
4. **Port Binding Failures**: Retry with exponential backoff or fail fast

## Logging

### Logger Implementation

```rust
#[derive(Clone)]
pub struct Logger {
    prefix: String,
}

impl Logger {
    pub fn new() -> Self {
        Self {
            prefix: format!("[{}arRPC{} > {}bridge{}]", 
                Self::rgb(88, 101, 242, ""),
                Self::reset_color(),
                Self::rgb(87, 242, 135, ""),
                Self::reset_color()),
        }
    }
    
    pub fn info(&self, message: &str) {
        println!("{} {}", self.prefix, message);
    }
    
    pub fn error(&self, message: &str) {
        eprintln!("{} ERROR: {}", self.prefix, message);
    }
    
    pub fn debug(&self, message: &str) {
        if std::env::var("ARRPC_DEBUG").is_ok() {
            println!("{} DEBUG: {}", self.prefix, message);
        }
    }
    
    fn rgb(r: u8, g: u8, b: u8, text: &str) -> String {
        format!("\x1b[38;2;{};{};{}m{}", r, g, b, text)
    }
    
    fn reset_color() -> &'static str {
        "\x1b[0m"
    }
}
```

## Dependencies

Add to `Cargo.toml`:

```toml
[dependencies]
tokio = { version = "1.0", features = ["full"] }
# Recommended: Use Axum for WebSocket server
axum = { version = "0.7", features = ["ws"] }
tower = "0.4"
# Alternative: Direct tokio-tungstenite
tokio-tungstenite = "0.20"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "1.0"
futures-util = "0.3"
```

## Testing Strategy

### Unit Tests

1. **Configuration parsing** - Test environment variable handling
2. **Message serialization** - Test JSON encoding/decoding
3. **Cache operations** - Test message storage and retrieval
4. **Error handling** - Test various failure scenarios

### Integration Tests

1. **WebSocket connectivity** - Test client connection/disconnection
2. **Message broadcasting** - Test activity forwarding to multiple clients
3. **Catch-up functionality** - Test sending cached messages to new clients
4. **Configuration variations** - Test different port configurations

### Load Testing

1. **Multiple client connections** - Test with 100+ simultaneous web clients
2. **High message throughput** - Test rapid activity updates
3. **Memory usage** - Monitor cache growth with many socket IDs
4. **Connection stability** - Test long-running connections

## Security Considerations

### Network Security

1. **Bind to localhost only** - Default to 127.0.0.1 to prevent external access
2. **No authentication required** - Local-only bridge doesn't need auth
3. **Input validation** - Validate all incoming activity data
4. **Rate limiting** - Consider limiting message frequency per client

### Memory Management

1. **Cache size limits** - Implement LRU eviction for old socket IDs
2. **Connection limits** - Limit maximum concurrent web clients
3. **Message size limits** - Validate activity data size before caching

## Performance Optimizations

### Efficiency Improvements

1. **Message pooling** - Reuse allocated message objects
2. **Batch broadcasting** - Group rapid updates for better throughput
3. **Async I/O** - Use Tokio for non-blocking operations
4. **Memory optimization** - Use efficient data structures for client tracking

### Monitoring

1. **Connection metrics** - Track active client count
2. **Message metrics** - Monitor message send/receive rates
3. **Error rates** - Track connection failures and timeouts
4. **Memory usage** - Monitor cache size and growth patterns

## Implementation Timeline

### Phase 1: Core Infrastructure (Week 1)
- Basic WebSocket server setup
- Configuration management
- Logging system
- Basic error handling

### Phase 2: Message Handling (Week 2)
- Activity message data structures
- Message cache implementation
- Basic broadcasting functionality
- Client connection management

### Phase 3: Integration (Week 3)
- Integration with main RPC server
- Catch-up message functionality
- Comprehensive error handling
- Performance optimizations

### Phase 4: Testing & Polish (Week 4)
- Unit and integration tests
- Load testing
- Documentation
- Code review and refinement

## Maintenance Considerations

### Monitoring and Debugging

1. **Health checks** - Endpoint to verify bridge server status
2. **Metrics collection** - Client count, message rates, error rates
3. **Debug logging** - Configurable verbose logging for troubleshooting
4. **Connection diagnostics** - Tools to inspect active client connections

### Future Enhancements

1. **Message filtering** - Allow clients to subscribe to specific socket IDs
2. **Compression** - WebSocket message compression for large activities
3. **Reconnection handling** - Automatic reconnection for dropped clients
4. **Multiple bridge instances** - Load balancing across multiple bridge servers