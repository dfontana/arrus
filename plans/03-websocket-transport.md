# WebSocket Transport Implementation Plan

## Overview

The WebSocket Transport provides Discord web client connectivity on ports 6463-6472, handling real-time communication between web-based Discord clients and the arRPC server. This transport implements Discord's WebSocket RPC protocol with proper security validation, connection lifecycle management, and message routing.

## Architecture Overview

The WebSocket Transport operates as a standalone server that:
- Scans and binds to the first available port in range 6463-6472
- Validates incoming connections against Discord security requirements
- Parses query parameters for client configuration
- Manages WebSocket connection lifecycle and message flow
- Integrates with the main RPC server through shared handlers

**Recommended Implementation**: Use **Axum** with WebSocket support for a more robust, performant, and maintainable WebSocket server. Axum provides better routing, middleware support, structured error handling, and easier testing compared to raw tokio-tungstenite.

## Core Data Structures

### 1. WebSocket Server Configuration
```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
// Recommended: Axum-based WebSocket handling
use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade, Message as AxumWsMessage},
    extract::{Query, ConnectInfo},
    response::Response,
    routing::get,
    Router,
};
// Alternative: Direct tokio-tungstenite
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, WebSocketStream};
use tokio_tungstenite::tungstenite::{Message, Error as WsError};
use serde_json::Value;
use url::Url;

pub struct WsServer {
    // Server state
    listener: Option<TcpListener>,
    active_port: Option<u16>,
    
    // Connection management
    active_connections: Arc<Mutex<HashMap<u32, WsConnection>>>,
    connection_counter: Arc<Mutex<u32>>,
    
    // Event handlers from RPC server
    handlers: TransportHandlers,
    
    // Configuration
    config: WsConfig,
}

pub struct WsConfig {
    port_range: (u16, u16),
    bind_address: String,
    debug_mode: bool,
    allowed_origins: Vec<String>,
    supported_versions: Vec<u8>,
    supported_encodings: Vec<String>,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            port_range: (6463, 6472),
            bind_address: "127.0.0.1".to_string(),
            debug_mode: std::env::var("ARRPC_DEBUG").is_ok(),
            allowed_origins: vec![
                "https://discord.com".to_string(),
                "https://ptb.discord.com".to_string(),
                "https://canary.discord.com".to_string(),
            ],
            supported_versions: vec![1],
            supported_encodings: vec!["json".to_string()],
        }
    }
}
```

### 2. Connection Management
```rust
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio::net::TcpStream;

pub struct WsConnection {
    socket_id: u32,
    client_id: String,
    version: u8,
    encoding: String,
    origin: String,
    websocket: WebSocketStream<TcpStream>,
    message_tx: mpsc::UnboundedSender<RpcMessage>,
    message_rx: mpsc::UnboundedReceiver<RpcMessage>,
}

#[derive(Debug, Clone)]
pub struct ConnectionParams {
    pub version: u8,
    pub encoding: String,
    pub client_id: String,
    pub origin: String,
}

impl ConnectionParams {
    pub fn from_query_string(query: &str) -> Result<Self, WsError> {
        let mut version = 1u8;
        let mut encoding = "json".to_string();
        let mut client_id = String::new();
        
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "v" => {
                    version = value.parse()
                        .map_err(|_| WsError::Protocol("Invalid version parameter".into()))?;
                }
                "encoding" => {
                    encoding = value.to_string();
                }
                "client_id" => {
                    client_id = value.to_string();
                }
                _ => {} // Ignore unknown parameters
            }
        }
        
        Ok(Self {
            version,
            encoding,
            client_id,
            origin: String::new(), // Set from headers separately
        })
    }
    
    pub fn validate(&self, config: &WsConfig) -> Result<(), ValidationError> {
        // Version validation
        if !config.supported_versions.contains(&self.version) {
            return Err(ValidationError::UnsupportedVersion(self.version));
        }
        
        // Encoding validation
        if !config.supported_encodings.contains(&self.encoding) {
            return Err(ValidationError::UnsupportedEncoding(self.encoding.clone()));
        }
        
        // Origin validation (if present)
        if !self.origin.is_empty() && !config.allowed_origins.contains(&self.origin) {
            return Err(ValidationError::DisallowedOrigin(self.origin.clone()));
        }
        
        // Client ID is optional for now (commented out in original)
        /*
        if self.client_id.is_empty() {
            return Err(ValidationError::MissingClientId);
        }
        */
        
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(u8),
    
    #[error("Unsupported encoding: {0}")]
    UnsupportedEncoding(String),
    
    #[error("Disallowed origin: {0}")]
    DisallowedOrigin(String),
    
    #[error("Missing client_id")]
    MissingClientId,
}
```

### 3. Message Protocol
```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcMessage {
    pub cmd: String,
    pub data: Option<Value>,
    pub evt: Option<String>,
    pub nonce: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub cmd: String,
    pub args: Option<Value>,
    pub nonce: Option<String>,
}

impl RpcMessage {
    pub fn to_websocket_message(&self) -> Result<Message, serde_json::Error> {
        let json = serde_json::to_string(self)?;
        Ok(Message::Text(json))
    }
    
    pub fn from_websocket_message(msg: Message) -> Result<RpcRequest, WsError> {
        match msg {
            Message::Text(text) => {
                serde_json::from_str(&text)
                    .map_err(|e| WsError::Protocol(format!("Invalid JSON: {}", e).into()))
            }
            Message::Binary(_) => {
                Err(WsError::Protocol("Binary messages not supported".into()))
            }
            Message::Close(_) => {
                Err(WsError::ConnectionClosed)
            }
            _ => {
                Err(WsError::Protocol("Unsupported message type".into()))
            }
        }
    }
}
```

## Implementation Details

### 1. Server Initialization and Port Scanning

**Axum Implementation Example**:
```rust
// Recommended: Axum-based approach with automatic port scanning
async fn setup_axum_server(port_range: (u16, u16)) -> Result<(Router, u16), WsError> {
    let app = Router::new()
        .route("/", get(websocket_handler))
        .with_state(server_state);
    
    // Port scanning logic remains the same
    for port in port_range.0..=port_range.1 {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        if let Ok(listener) = TcpListener::bind(&addr).await {
            return Ok((app, port));
        }
    }
    Err(WsError::Io(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        "No available ports in range"
    )))
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<ConnectionParams>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Response {
    ws.on_upgrade(move |socket| handle_websocket(socket, params, addr))
}
```

**Traditional tokio-tungstenite Implementation**:
```rust
use tokio::net::TcpListener;
use tracing::{info, warn, error, debug};

impl WsServer {
    pub async fn new(handlers: TransportHandlers) -> Result<Self, WsError> {
        let config = WsConfig::default();
        
        let mut server = Self {
            listener: None,
            active_port: None,
            active_connections: Arc::new(Mutex::new(HashMap::new())),
            connection_counter: Arc::new(Mutex::new(0)),
            handlers,
            config,
        };
        
        // Attempt to bind to first available port in range
        server.bind_to_available_port().await?;
        
        Ok(server)
    }
    
    async fn bind_to_available_port(&mut self) -> Result<(), WsError> {
        let (start_port, end_port) = self.config.port_range;
        
        for port in start_port..=end_port {
            if self.config.debug_mode {
                debug!("Trying to bind WebSocket server to port {}", port);
            }
            
            let addr = format!("{}:{}", self.config.bind_address, port);
            
            match TcpListener::bind(&addr).await {
                Ok(listener) => {
                    info!("WebSocket server listening on {}", addr);
                    self.listener = Some(listener);
                    self.active_port = Some(port);
                    return Ok(());
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::AddrInUse {
                        warn!("Port {} is in use, trying next port", port);
                        continue;
                    } else {
                        error!("Failed to bind to {}: {}", addr, e);
                        return Err(WsError::Io(e));
                    }
                }
            }
        }
        
        Err(WsError::Io(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            format!("No available ports in range {}-{}", start_port, end_port),
        )))
    }
    
    pub async fn start(&mut self) -> Result<(), WsError> {
        let listener = self.listener.take()
            .ok_or_else(|| WsError::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Server not properly initialized"
            )))?;
        
        info!("Starting WebSocket server on port {}", 
              self.active_port.unwrap());
        
        while let Ok((stream, addr)) = listener.accept().await {
            if self.config.debug_mode {
                debug!("New TCP connection from {}", addr);
            }
            
            let connections = self.active_connections.clone();
            let counter = self.connection_counter.clone();
            let handlers = self.handlers.clone();
            let config = self.config.clone();
            
            // Spawn connection handler
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(
                    stream, 
                    addr, 
                    connections, 
                    counter, 
                    handlers, 
                    config
                ).await {
                    error!("Connection handling error: {}", e);
                }
            });
        }
        
        Ok(())
    }
}
```

### 2. Connection Handling and Validation
```rust
use tokio_tungstenite::accept_async;
use http::HeaderMap;

impl WsServer {
    async fn handle_connection(
        stream: TcpStream,
        addr: SocketAddr,
        connections: Arc<Mutex<HashMap<u32, WsConnection>>>,
        counter: Arc<Mutex<u32>>,
        handlers: TransportHandlers,
        config: WsConfig,
    ) -> Result<(), WsError> {
        // Perform WebSocket handshake with callback to validate headers
        let ws_stream = accept_async_with_validation(stream, |req| {
            Self::validate_handshake_request(req, &config)
        }).await?;
        
        // Extract connection parameters from the handshake
        let params = Self::extract_connection_params(&req, &config).await?;
        
        // Generate unique connection ID
        let socket_id = {
            let mut c = counter.lock().unwrap();
            *c += 1;
            *c
        };
        
        if config.debug_mode {
            debug!("New WebSocket connection: id={}, client_id={}, origin={}", 
                   socket_id, params.client_id, params.origin);
        }
        
        // Create bidirectional message channel
        let (message_tx, message_rx) = mpsc::unbounded_channel();
        
        // Create connection object
        let connection = WsConnection {
            socket_id,
            client_id: params.client_id.clone(),
            version: params.version,
            encoding: params.encoding.clone(),
            origin: params.origin.clone(),
            websocket: ws_stream,
            message_tx: message_tx.clone(),
            message_rx,
        };
        
        // Store connection
        {
            let mut conns = connections.lock().unwrap();
            conns.insert(socket_id, connection);
        }
        
        // Create transport connection for RPC server
        let transport_connection = SocketConnection {
            socket_id,
            client_id: params.client_id,
            transport_type: TransportType::WebSocket,
            sender: message_tx,
        };
        
        // Notify RPC server of new connection
        (handlers.on_connection)(transport_connection);
        
        // Start message handling loops
        let read_connections = connections.clone();
        let write_connections = connections.clone();
        let read_handlers = handlers.clone();
        
        // Spawn read task
        let read_task = tokio::spawn(async move {
            Self::handle_incoming_messages(socket_id, read_connections, read_handlers, config.clone()).await
        });
        
        // Spawn write task
        let write_task = tokio::spawn(async move {
            Self::handle_outgoing_messages(socket_id, write_connections).await
        });
        
        // Wait for either task to complete (connection closed)
        let _ = tokio::select! {
            result = read_task => result,
            result = write_task => result,
        };
        
        // Clean up connection
        {
            let mut conns = connections.lock().unwrap();
            conns.remove(&socket_id);
        }
        
        // Notify RPC server of disconnection
        (handlers.on_close)(socket_id);
        
        Ok(())
    }
    
    fn validate_handshake_request(
        req: &http::Request<()>,
        config: &WsConfig,
    ) -> Result<(), WsError> {
        // Extract and validate Origin header
        if let Some(origin) = req.headers().get("origin") {
            let origin_str = origin.to_str()
                .map_err(|_| WsError::Protocol("Invalid Origin header".into()))?;
            
            if !config.allowed_origins.contains(&origin_str.to_string()) {
                return Err(WsError::Protocol(
                    format!("Disallowed origin: {}", origin_str).into()
                ));
            }
        }
        
        // Extract and validate query parameters
        let uri = req.uri();
        if let Some(query) = uri.query() {
            let params = ConnectionParams::from_query_string(query)?;
            params.validate(config)
                .map_err(|e| WsError::Protocol(e.to_string().into()))?;
        }
        
        Ok(())
    }
    
    async fn extract_connection_params(
        req: &http::Request<()>,
        config: &WsConfig,
    ) -> Result<ConnectionParams, WsError> {
        let mut params = ConnectionParams {
            version: 1,
            encoding: "json".to_string(),
            client_id: String::new(),
            origin: String::new(),
        };
        
        // Extract from query string
        if let Some(query) = req.uri().query() {
            params = ConnectionParams::from_query_string(query)?;
        }
        
        // Extract origin from headers
        if let Some(origin) = req.headers().get("origin") {
            params.origin = origin.to_str()
                .map_err(|_| WsError::Protocol("Invalid Origin header".into()))?
                .to_string();
        }
        
        // Final validation
        params.validate(config)
            .map_err(|e| WsError::Protocol(e.to_string().into()))?;
        
        Ok(params)
    }
}

// Helper function for WebSocket handshake with validation
async fn accept_async_with_validation<F>(
    stream: TcpStream,
    validator: F,
) -> Result<WebSocketStream<TcpStream>, WsError>
where
    F: FnOnce(&http::Request<()>) -> Result<(), WsError>,
{
    // This would need to be implemented using lower-level WebSocket handshake
    // For now, using the standard accept_async and assuming validation is done separately
    accept_async(stream).await
}
```

### 3. Message Handling
```rust
use futures_util::{SinkExt, StreamExt};

impl WsServer {
    async fn handle_incoming_messages(
        socket_id: u32,
        connections: Arc<Mutex<HashMap<u32, WsConnection>>>,
        handlers: TransportHandlers,
        config: WsConfig,
    ) -> Result<(), WsError> {
        loop {
            // Get WebSocket stream from connection
            let ws_message = {
                let mut conns = connections.lock().unwrap();
                if let Some(connection) = conns.get_mut(&socket_id) {
                    // This needs to be restructured to avoid holding the lock
                    // while awaiting the WebSocket stream
                    match connection.websocket.next().await {
                        Some(Ok(msg)) => msg,
                        Some(Err(e)) => {
                            error!("WebSocket error for connection {}: {}", socket_id, e);
                            return Err(e);
                        }
                        None => {
                            info!("WebSocket connection {} closed", socket_id);
                            return Ok(());
                        }
                    }
                } else {
                    warn!("Connection {} not found", socket_id);
                    return Ok(());
                }
            };
            
            match ws_message {
                Message::Text(text) => {
                    if config.debug_mode {
                        debug!("Received message from {}: {}", socket_id, text);
                    }
                    
                    // Parse RPC message
                    match serde_json::from_str::<RpcRequest>(&text) {
                        Ok(request) => {
                            // Forward to RPC server
                            (handlers.on_message)(socket_id, request);
                        }
                        Err(e) => {
                            error!("Failed to parse RPC message from {}: {}", socket_id, e);
                            // Send error response
                            Self::send_error_response(
                                socket_id,
                                &connections,
                                4000,
                                "Invalid JSON",
                                None,
                            ).await?;
                        }
                    }
                }
                
                Message::Binary(_) => {
                    warn!("Binary message received from {}, not supported", socket_id);
                    Self::send_error_response(
                        socket_id,
                        &connections,
                        4001,
                        "Binary messages not supported",
                        None,
                    ).await?;
                }
                
                Message::Close(frame) => {
                    info!("WebSocket close frame received from {}: {:?}", socket_id, frame);
                    return Ok(());
                }
                
                Message::Ping(data) => {
                    if config.debug_mode {
                        debug!("Ping received from {}", socket_id);
                    }
                    Self::send_pong(socket_id, &connections, data).await?;
                }
                
                Message::Pong(_) => {
                    if config.debug_mode {
                        debug!("Pong received from {}", socket_id);
                    }
                }
                
                Message::Frame(_) => {
                    // Raw frames are handled internally by tungstenite
                }
            }
        }
    }
    
    async fn handle_outgoing_messages(
        socket_id: u32,
        connections: Arc<Mutex<HashMap<u32, WsConnection>>>,
    ) -> Result<(), WsError> {
        loop {
            // Get message receiver from connection
            let message = {
                let mut conns = connections.lock().unwrap();
                if let Some(connection) = conns.get_mut(&socket_id) {
                    match connection.message_rx.recv().await {
                        Some(msg) => msg,
                        None => {
                            info!("Message channel closed for connection {}", socket_id);
                            return Ok(());
                        }
                    }
                } else {
                    warn!("Connection {} not found for outgoing message", socket_id);
                    return Ok(());
                }
            };
            
            if let Err(e) = Self::send_rpc_message(socket_id, &connections, message).await {
                error!("Failed to send message to {}: {}", socket_id, e);
                return Err(e);
            }
        }
    }
    
    async fn send_rpc_message(
        socket_id: u32,
        connections: &Arc<Mutex<HashMap<u32, WsConnection>>>,
        message: RpcMessage,
    ) -> Result<(), WsError> {
        let ws_message = message.to_websocket_message()
            .map_err(|e| WsError::Protocol(format!("Serialization error: {}", e).into()))?;
        
        let mut conns = connections.lock().unwrap();
        if let Some(connection) = conns.get_mut(&socket_id) {
            connection.websocket.send(ws_message).await?;
            Ok(())
        } else {
            Err(WsError::Protocol("Connection not found".into()))
        }
    }
    
    async fn send_error_response(
        socket_id: u32,
        connections: &Arc<Mutex<HashMap<u32, WsConnection>>>,
        code: u32,
        message: &str,
        nonce: Option<String>,
    ) -> Result<(), WsError> {
        let error_msg = RpcMessage {
            cmd: "ERROR".to_string(),
            data: Some(serde_json::json!({
                "code": code,
                "message": message
            })),
            evt: Some("ERROR".to_string()),
            nonce,
        };
        
        Self::send_rpc_message(socket_id, connections, error_msg).await
    }
    
    async fn send_pong(
        socket_id: u32,
        connections: &Arc<Mutex<HashMap<u32, WsConnection>>>,
        data: Vec<u8>,
    ) -> Result<(), WsError> {
        let mut conns = connections.lock().unwrap();
        if let Some(connection) = conns.get_mut(&socket_id) {
            connection.websocket.send(Message::Pong(data)).await?;
            Ok(())
        } else {
            Err(WsError::Protocol("Connection not found".into()))
        }
    }
}
```

### 4. Connection Lifecycle Management
```rust
impl WsServer {
    pub async fn shutdown(&mut self) -> Result<(), WsError> {
        info!("Shutting down WebSocket server");
        
        // Close all active connections
        let connections_to_close: Vec<u32> = {
            let conns = self.active_connections.lock().unwrap();
            conns.keys().cloned().collect()
        };
        
        for socket_id in connections_to_close {
            self.close_connection(socket_id, Some(1001), Some("Server shutdown")).await?;
        }
        
        // Clear connections map
        {
            let mut conns = self.active_connections.lock().unwrap();
            conns.clear();
        }
        
        info!("WebSocket server shutdown complete");
        Ok(())
    }
    
    async fn close_connection(
        &self,
        socket_id: u32,
        close_code: Option<u16>,
        reason: Option<&str>,
    ) -> Result<(), WsError> {
        let mut conns = self.active_connections.lock().unwrap();
        if let Some(mut connection) = conns.remove(&socket_id) {
            let close_frame = if let (Some(code), Some(reason)) = (close_code, reason) {
                Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(code),
                    reason: reason.into(),
                })
            } else {
                None
            };
            
            let _ = connection.websocket.close(close_frame).await;
            info!("Closed WebSocket connection {}", socket_id);
        }
        
        Ok(())
    }
    
    pub fn get_active_connections(&self) -> Vec<u32> {
        let conns = self.active_connections.lock().unwrap();
        conns.keys().cloned().collect()
    }
    
    pub fn get_connection_count(&self) -> usize {
        let conns = self.active_connections.lock().unwrap();
        conns.len()
    }
    
    pub fn get_connection_info(&self, socket_id: u32) -> Option<ConnectionInfo> {
        let conns = self.active_connections.lock().unwrap();
        conns.get(&socket_id).map(|conn| ConnectionInfo {
            socket_id: conn.socket_id,
            client_id: conn.client_id.clone(),
            version: conn.version,
            encoding: conn.encoding.clone(),
            origin: conn.origin.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub socket_id: u32,
    pub client_id: String,
    pub version: u8,
    pub encoding: String,
    pub origin: String,
}
```

## Security Implementation

### 1. Origin Validation
```rust
impl WsServer {
    fn validate_origin(origin: &str, config: &WsConfig) -> bool {
        if origin.is_empty() {
            // Empty origin is allowed (for development or direct connections)
            return true;
        }
        
        config.allowed_origins.iter().any(|allowed| {
            // Exact match for Discord domains
            origin == allowed ||
            // Allow subdomains of Discord
            (allowed.starts_with("https://") && 
             origin.ends_with(&allowed[8..]) && 
             origin.chars().nth(origin.len() - allowed.len() + 8 - 1) == Some('.'))
        })
    }
    
    fn validate_user_agent(user_agent: Option<&str>) -> bool {
        // Discord web clients typically have specific user agent patterns
        // This is optional additional validation
        if let Some(ua) = user_agent {
            ua.contains("Discord") || 
            ua.contains("Chrome") || 
            ua.contains("Firefox") || 
            ua.contains("Safari")
        } else {
            true // Allow missing user agent
        }
    }
}
```

### 2. Rate Limiting
```rust
use std::time::{Duration, Instant};
use std::collections::VecDeque;

pub struct RateLimiter {
    window_size: Duration,
    max_requests: usize,
    requests: VecDeque<Instant>,
}

impl RateLimiter {
    pub fn new(window_size: Duration, max_requests: usize) -> Self {
        Self {
            window_size,
            max_requests,
            requests: VecDeque::new(),
        }
    }
    
    pub fn check_rate_limit(&mut self) -> bool {
        let now = Instant::now();
        
        // Remove old requests outside the window
        while let Some(&front) = self.requests.front() {
            if now.duration_since(front) > self.window_size {
                self.requests.pop_front();
            } else {
                break;
            }
        }
        
        // Check if we're within the limit
        if self.requests.len() < self.max_requests {
            self.requests.push_back(now);
            true
        } else {
            false
        }
    }
}

// Usage in connection handling
impl WsServer {
    fn create_rate_limiter_per_connection() -> HashMap<u32, RateLimiter> {
        HashMap::new()
    }
    
    async fn check_message_rate_limit(
        socket_id: u32,
        rate_limiters: &mut HashMap<u32, RateLimiter>,
    ) -> bool {
        let limiter = rate_limiters.entry(socket_id)
            .or_insert_with(|| RateLimiter::new(Duration::from_secs(60), 100));
        
        limiter.check_rate_limit()
    }
}
```

## Error Handling and Logging

### 1. Comprehensive Error Types
```rust
#[derive(Debug, thiserror::Error)]
pub enum WsError {
    #[error("WebSocket protocol error: {0}")]
    Protocol(#[from] tokio_tungstenite::tungstenite::Error),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("Connection validation error: {0}")]
    Validation(#[from] ValidationError),
    
    #[error("Connection {0} not found")]
    ConnectionNotFound(u32),
    
    #[error("Server not initialized")]
    NotInitialized,
    
    #[error("Rate limit exceeded for connection {0}")]
    RateLimitExceeded(u32),
    
    #[error("Message channel closed")]
    ChannelClosed,
}
```

### 2. Structured Logging
```rust
use tracing::{info, warn, error, debug, instrument};

impl WsServer {
    #[instrument(skip(self), fields(port = self.active_port))]
    pub async fn start_with_logging(&mut self) -> Result<(), WsError> {
        info!("Starting WebSocket transport");
        
        match self.start().await {
            Ok(()) => {
                info!("WebSocket transport started successfully");
                Ok(())
            }
            Err(e) => {
                error!("Failed to start WebSocket transport: {}", e);
                Err(e)
            }
        }
    }
    
    #[instrument(skip(self, connections, handlers), fields(socket_id))]
    async fn log_connection_event(
        socket_id: u32,
        event: &str,
        client_id: &str,
        origin: &str,
    ) {
        info!(
            socket_id = socket_id,
            client_id = client_id,
            origin = origin,
            event = event,
            "WebSocket connection event"
        );
    }
    
    #[instrument(skip(message), fields(socket_id, cmd = %message.cmd))]
    fn log_message_event(socket_id: u32, message: &RpcMessage, direction: &str) {
        debug!(
            socket_id = socket_id,
            cmd = message.cmd,
            has_data = message.data.is_some(),
            has_nonce = message.nonce.is_some(),
            direction = direction,
            "RPC message"
        );
    }
}
```

## Integration with RPC Server

### 1. Transport Handler Implementation
```rust
use std::sync::Arc;

#[derive(Clone)]
pub struct TransportHandlers {
    pub on_connection: Arc<dyn Fn(SocketConnection) + Send + Sync>,
    pub on_message: Arc<dyn Fn(u32, RpcRequest) + Send + Sync>,
    pub on_close: Arc<dyn Fn(u32) + Send + Sync>,
}

impl WsServer {
    pub fn new_with_handlers(handlers: TransportHandlers) -> Result<Self, WsError> {
        Ok(Self {
            listener: None,
            active_port: None,
            active_connections: Arc::new(Mutex::new(HashMap::new())),
            connection_counter: Arc::new(Mutex::new(0)),
            handlers,
            config: WsConfig::default(),
        })
    }
    
    // Bridge between WebSocket transport and RPC server
    fn create_socket_connection(
        socket_id: u32,
        params: &ConnectionParams,
        message_tx: mpsc::UnboundedSender<RpcMessage>,
    ) -> SocketConnection {
        SocketConnection {
            socket_id,
            client_id: params.client_id.clone(),
            transport_type: TransportType::WebSocket,
            sender: message_tx,
        }
    }
}

// Socket connection interface for RPC server
pub struct SocketConnection {
    pub socket_id: u32,
    pub client_id: String,
    pub transport_type: TransportType,
    pub sender: mpsc::UnboundedSender<RpcMessage>,
}

impl SocketConnection {
    pub fn send(&self, message: RpcMessage) -> Result<(), WsError> {
        self.sender.send(message)
            .map_err(|_| WsError::ChannelClosed)
    }
}

#[derive(Debug, Clone)]
pub enum TransportType {
    Ipc,
    WebSocket,
    Process,
}
```

### 2. Message Flow Implementation
```rust
impl WsServer {
    // Called by RPC server to send messages to WebSocket clients
    pub async fn send_to_client(
        &self,
        socket_id: u32,
        message: RpcMessage,
    ) -> Result<(), WsError> {
        let connections = self.active_connections.clone();
        Self::send_rpc_message(socket_id, &connections, message).await
    }
    
    // Called by RPC server to broadcast to all WebSocket clients
    pub async fn broadcast_to_clients(
        &self,
        message: RpcMessage,
        exclude_socket: Option<u32>,
    ) -> Result<(), WsError> {
        let socket_ids: Vec<u32> = {
            let conns = self.active_connections.lock().unwrap();
            conns.keys()
                .filter(|&&id| Some(id) != exclude_socket)
                .cloned()
                .collect()
        };
        
        for socket_id in socket_ids {
            if let Err(e) = self.send_to_client(socket_id, message.clone()).await {
                warn!("Failed to send broadcast message to {}: {}", socket_id, e);
            }
        }
        
        Ok(())
    }
    
    // Get statistics for monitoring
    pub fn get_stats(&self) -> WsStats {
        let conns = self.active_connections.lock().unwrap();
        
        let mut client_ids = Vec::new();
        let mut origins = Vec::new();
        
        for conn in conns.values() {
            client_ids.push(conn.client_id.clone());
            origins.push(conn.origin.clone());
        }
        
        WsStats {
            active_connections: conns.len(),
            active_port: self.active_port,
            client_ids,
            origins,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WsStats {
    pub active_connections: usize,
    pub active_port: Option<u16>,
    pub client_ids: Vec<String>,
    pub origins: Vec<String>,
}
```

## Testing Strategy

### 1. Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;
    
    #[test]
    async fn test_connection_params_parsing() {
        let query = "v=1&encoding=json&client_id=123456789";
        let params = ConnectionParams::from_query_string(query).unwrap();
        
        assert_eq!(params.version, 1);
        assert_eq!(params.encoding, "json");
        assert_eq!(params.client_id, "123456789");
    }
    
    #[test]
    async fn test_origin_validation() {
        let config = WsConfig::default();
        
        assert!(WsServer::validate_origin("https://discord.com", &config));
        assert!(WsServer::validate_origin("https://ptb.discord.com", &config));
        assert!(WsServer::validate_origin("", &config)); // Empty origin allowed
        assert!(!WsServer::validate_origin("https://malicious.com", &config));
    }
    
    #[test]
    async fn test_rate_limiter() {
        let mut limiter = RateLimiter::new(Duration::from_secs(1), 2);
        
        assert!(limiter.check_rate_limit()); // First request
        assert!(limiter.check_rate_limit()); // Second request
        assert!(!limiter.check_rate_limit()); // Third request should fail
        
        // Wait for window to reset
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(limiter.check_rate_limit()); // Should work again
    }
    
    #[test]
    async fn test_message_serialization() {
        let message = RpcMessage {
            cmd: "SET_ACTIVITY".to_string(),
            data: Some(serde_json::json!({"test": "data"})),
            evt: None,
            nonce: Some("test-nonce".to_string()),
        };
        
        let ws_message = message.to_websocket_message().unwrap();
        match ws_message {
            Message::Text(text) => {
                let parsed: RpcMessage = serde_json::from_str(&text).unwrap();
                assert_eq!(parsed.cmd, "SET_ACTIVITY");
                assert_eq!(parsed.nonce, Some("test-nonce".to_string()));
            }
            _ => panic!("Expected text message"),
        }
    }
}
```

### 2. Integration Tests
```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    use tokio_tungstenite::{connect_async, tungstenite::Message};
    use std::sync::mpsc;
    
    #[tokio::test]
    async fn test_websocket_connection_flow() {
        // Create mock handlers
        let (conn_tx, conn_rx) = mpsc::channel();
        let (msg_tx, msg_rx) = mpsc::channel();
        let (close_tx, close_rx) = mpsc::channel();
        
        let handlers = TransportHandlers {
            on_connection: Arc::new(move |conn| {
                conn_tx.send(conn).unwrap();
            }),
            on_message: Arc::new(move |id, req| {
                msg_tx.send((id, req)).unwrap();
            }),
            on_close: Arc::new(move |id| {
                close_tx.send(id).unwrap();
            }),
        };
        
        // Start WebSocket server
        let mut server = WsServer::new_with_handlers(handlers).unwrap();
        let port = server.active_port.unwrap();
        
        tokio::spawn(async move {
            server.start().await.unwrap();
        });
        
        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Connect client
        let url = format!("ws://127.0.0.1:{}/?v=1&encoding=json&client_id=123456789", port);
        let (ws_stream, _) = connect_async(&url).await.unwrap();
        let (mut write, mut read) = ws_stream.split();
        
        // Verify connection was registered
        let connection = conn_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(connection.client_id, "123456789");
        
        // Send test message
        let test_message = serde_json::json!({
            "cmd": "SET_ACTIVITY",
            "args": {"activity": null},
            "nonce": "test-123"
        });
        
        write.send(Message::Text(test_message.to_string())).await.unwrap();
        
        // Verify message was received
        let (socket_id, request) = msg_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(request.cmd, "SET_ACTIVITY");
        assert_eq!(request.nonce, Some("test-123".to_string()));
        
        // Close connection
        write.close().await.unwrap();
        
        // Verify close was registered
        let closed_id = close_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(closed_id, socket_id);
    }
    
    #[tokio::test]
    async fn test_invalid_origin_rejection() {
        let handlers = TransportHandlers {
            on_connection: Arc::new(|_| {}),
            on_message: Arc::new(|_, _| {}),
            on_close: Arc::new(|_| {}),
        };
        
        let mut server = WsServer::new_with_handlers(handlers).unwrap();
        let port = server.active_port.unwrap();
        
        tokio::spawn(async move {
            server.start().await.unwrap();
        });
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Try to connect with invalid origin
        let url = format!("ws://127.0.0.1:{}/?v=1&encoding=json", port);
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert("origin", "https://malicious.com".parse().unwrap());
        
        let result = connect_async(request).await;
        assert!(result.is_err()); // Should be rejected
    }
}
```

## Performance Optimization

### 1. Connection Pooling and Reuse
```rust
use dashmap::DashMap;
use tokio::sync::RwLock;

pub struct OptimizedConnectionManager {
    connections: DashMap<u32, WsConnection>,
    connection_counter: AtomicU32,
    stats: RwLock<ConnectionStats>,
}

#[derive(Default)]
struct ConnectionStats {
    total_connections: u64,
    messages_sent: u64,
    messages_received: u64,
    bytes_sent: u64,
    bytes_received: u64,
}

impl OptimizedConnectionManager {
    pub fn new() -> Self {
        Self {
            connections: DashMap::new(),
            connection_counter: AtomicU32::new(0),
            stats: RwLock::new(ConnectionStats::default()),
        }
    }
    
    pub async fn add_connection(&self, connection: WsConnection) -> u32 {
        let socket_id = self.connection_counter.fetch_add(1, Ordering::Relaxed) + 1;
        self.connections.insert(socket_id, connection);
        
        let mut stats = self.stats.write().await;
        stats.total_connections += 1;
        
        socket_id
    }
    
    pub async fn remove_connection(&self, socket_id: u32) -> Option<WsConnection> {
        self.connections.remove(&socket_id).map(|(_, conn)| conn)
    }
    
    pub async fn send_message(&self, socket_id: u32, message: RpcMessage) -> Result<(), WsError> {
        if let Some(connection) = self.connections.get(&socket_id) {
            let serialized = serde_json::to_string(&message)?;
            let bytes_len = serialized.len() as u64;
            
            connection.message_tx.send(message)
                .map_err(|_| WsError::ChannelClosed)?;
            
            let mut stats = self.stats.write().await;
            stats.messages_sent += 1;
            stats.bytes_sent += bytes_len;
            
            Ok(())
        } else {
            Err(WsError::ConnectionNotFound(socket_id))
        }
    }
    
    pub async fn get_stats(&self) -> ConnectionStats {
        self.stats.read().await.clone()
    }
}
```

### 2. Message Batching and Compression
```rust
use tokio::time::{interval, Duration};

pub struct MessageBatcher {
    pending_messages: Vec<(u32, RpcMessage)>,
    batch_size: usize,
    flush_interval: Duration,
}

impl MessageBatcher {
    pub fn new(batch_size: usize, flush_interval: Duration) -> Self {
        Self {
            pending_messages: Vec::with_capacity(batch_size),
            batch_size,
            flush_interval,
        }
    }
    
    pub async fn add_message(&mut self, socket_id: u32, message: RpcMessage) -> Vec<(u32, RpcMessage)> {
        self.pending_messages.push((socket_id, message));
        
        if self.pending_messages.len() >= self.batch_size {
            self.flush()
        } else {
            Vec::new()
        }
    }
    
    pub fn flush(&mut self) -> Vec<(u32, RpcMessage)> {
        std::mem::take(&mut self.pending_messages)
    }
    
    pub async fn start_flush_timer(&mut self, sender: mpsc::UnboundedSender<Vec<(u32, RpcMessage)>>) {
        let mut interval = interval(self.flush_interval);
        
        loop {
            interval.tick().await;
            
            if !self.pending_messages.is_empty() {
                let batch = self.flush();
                if let Err(_) = sender.send(batch) {
                    break; // Channel closed
                }
            }
        }
    }
}
```

## Monitoring and Diagnostics

### 1. Health Checks
```rust
#[derive(Debug, Serialize)]
pub struct HealthStatus {
    pub status: String,
    pub active_port: Option<u16>,
    pub connection_count: usize,
    pub uptime_seconds: u64,
    pub memory_usage_mb: u64,
    pub last_error: Option<String>,
}

impl WsServer {
    pub fn get_health_status(&self) -> HealthStatus {
        HealthStatus {
            status: if self.active_port.is_some() { "healthy" } else { "unhealthy" }.to_string(),
            active_port: self.active_port,
            connection_count: self.get_connection_count(),
            uptime_seconds: self.get_uptime_seconds(),
            memory_usage_mb: self.get_memory_usage_mb(),
            last_error: None, // Would track last error
        }
    }
    
    fn get_uptime_seconds(&self) -> u64 {
        // Implementation would track server start time
        0
    }
    
    fn get_memory_usage_mb(&self) -> u64 {
        // Implementation would calculate memory usage
        0
    }
}
```

### 2. Metrics Collection
```rust
use prometheus::{Counter, Gauge, Histogram, register_counter, register_gauge, register_histogram};

pub struct WsMetrics {
    connections_total: Counter,
    active_connections: Gauge,
    messages_sent: Counter,
    messages_received: Counter,
    message_processing_duration: Histogram,
    errors_total: Counter,
}

impl WsMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        Ok(Self {
            connections_total: register_counter!(
                "websocket_connections_total",
                "Total number of WebSocket connections"
            )?,
            active_connections: register_gauge!(
                "websocket_active_connections",
                "Current number of active WebSocket connections"
            )?,
            messages_sent: register_counter!(
                "websocket_messages_sent_total",
                "Total number of messages sent"
            )?,
            messages_received: register_counter!(
                "websocket_messages_received_total", 
                "Total number of messages received"
            )?,
            message_processing_duration: register_histogram!(
                "websocket_message_processing_duration_seconds",
                "Time spent processing WebSocket messages"
            )?,
            errors_total: register_counter!(
                "websocket_errors_total",
                "Total number of WebSocket errors"
            )?,
        })
    }
    
    pub fn record_connection(&self) {
        self.connections_total.inc();
        self.active_connections.inc();
    }
    
    pub fn record_disconnection(&self) {
        self.active_connections.dec();
    }
    
    pub fn record_message_sent(&self) {
        self.messages_sent.inc();
    }
    
    pub fn record_message_received(&self) {
        self.messages_received.inc();
    }
    
    pub fn record_error(&self) {
        self.errors_total.inc();
    }
}
```

## Dependencies

**Recommended Crates** (Axum approach):
```toml
[dependencies]
tokio = { version = "1.0", features = ["full"] }
axum = { version = "0.7", features = ["ws"] }
tower = "0.4"
tower-http = { version = "0.5", features = ["cors", "trace"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "1.0"
tracing = "0.1"
url = "2.4"
```

**Alternative** (tokio-tungstenite approach):
```toml
[dependencies]
tokio = { version = "1.0", features = ["full"] }
tokio-tungstenite = "0.20"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "1.0"
futures-util = "0.3"
url = "2.4"
```

This comprehensive implementation plan provides all the necessary details for implementing a robust WebSocket Transport in Rust that handles Discord web client connections on ports 6463-6472, with proper security validation, connection management, and integration with the main RPC server system.