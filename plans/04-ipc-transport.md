# IPC Transport Implementation Plan

## Overview

The IPC Transport provides Discord native client connectivity via Unix domain sockets, handling real-time communication between Discord desktop applications and the arRPC server. This transport implements Discord's binary IPC protocol with socket path discovery, binary framing, handshake validation, and connection lifecycle management.

## Architecture Overview

The IPC Transport operates as a standalone server that:
- Discovers and binds to available Discord IPC socket paths (/tmp/discord-ipc-0 through -9)
- Implements Discord's binary framing protocol with 8-byte headers
- Validates connection handshakes with version and client ID verification
- Manages Unix domain socket lifecycle and message flow
- Integrates with the main RPC server through shared handlers

## Core Data Structures

### 1. IPC Server Configuration
```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use tokio::net::{UnixListener, UnixStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde_json::Value;
use std::os::unix::fs::PermissionsExt;
use std::fs;

pub struct IpcServer {
    // Server state
    listener: Option<UnixListener>,
    socket_path: Option<PathBuf>,
    
    // Connection management
    active_connections: Arc<Mutex<HashMap<u32, IpcConnection>>>,
    connection_counter: Arc<Mutex<u32>>,
    unique_id_counter: Arc<Mutex<u32>>,
    
    // Event handlers from RPC server
    handlers: TransportHandlers,
    
    // Configuration
    config: IpcConfig,
}

pub struct IpcConfig {
    socket_base_path: String,
    max_socket_tries: u8,
    debug_mode: bool,
    supported_versions: Vec<u8>,
    connection_timeout_ms: u64,
    ping_timeout_ms: u64,
}

impl Default for IpcConfig {
    fn default() -> Self {
        let base_path = std::env::var("XDG_RUNTIME_DIR")
            .or_else(|_| std::env::var("TMPDIR"))
            .or_else(|_| std::env::var("TMP"))
            .or_else(|_| std::env::var("TEMP"))
            .unwrap_or_else(|_| "/tmp".to_string());
        
        Self {
            socket_base_path: format!("{}/discord-ipc", base_path),
            max_socket_tries: 10,
            debug_mode: std::env::var("ARRPC_DEBUG").is_ok(),
            supported_versions: vec![1],
            connection_timeout_ms: 5000,
            ping_timeout_ms: 1000,
        }
    }
}
```

### 2. Binary Protocol Implementation
```rust
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Write};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IpcPacketType {
    Handshake = 0,
    Frame = 1,
    Close = 2,
    Ping = 3,
    Pong = 4,
}

impl TryFrom<u32> for IpcPacketType {
    type Error = IpcError;
    
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(IpcPacketType::Handshake),
            1 => Ok(IpcPacketType::Frame),
            2 => Ok(IpcPacketType::Close),
            3 => Ok(IpcPacketType::Ping),
            4 => Ok(IpcPacketType::Pong),
            _ => Err(IpcError::InvalidPacketType(value)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IpcPacket {
    pub packet_type: IpcPacketType,
    pub data: Value,
}

impl IpcPacket {
    pub fn encode(&self) -> Result<Vec<u8>, IpcError> {
        let json_data = serde_json::to_string(&self.data)?;
        let data_bytes = json_data.as_bytes();
        let data_size = data_bytes.len() as u32;
        
        let mut buffer = Vec::with_capacity(8 + data_bytes.len());
        
        // Write packet type (4 bytes, little endian)
        buffer.write_u32::<LittleEndian>(self.packet_type as u32)?;
        
        // Write data size (4 bytes, little endian)
        buffer.write_u32::<LittleEndian>(data_size)?;
        
        // Write JSON data
        buffer.write_all(data_bytes)?;
        
        Ok(buffer)
    }
    
    pub fn decode(buffer: &[u8]) -> Result<Self, IpcError> {
        if buffer.len() < 8 {
            return Err(IpcError::InsufficientData);
        }
        
        let mut cursor = Cursor::new(buffer);
        
        // Read packet type (4 bytes, little endian)
        let packet_type_raw = cursor.read_u32::<LittleEndian>()?;
        let packet_type = IpcPacketType::try_from(packet_type_raw)?;
        
        // Read data size (4 bytes, little endian)
        let data_size = cursor.read_u32::<LittleEndian>()?;
        
        // Validate buffer has enough data
        let remaining_data = &buffer[8..];
        if remaining_data.len() < data_size as usize {
            return Err(IpcError::InsufficientData);
        }
        
        // Extract and parse JSON data
        let json_bytes = &remaining_data[..data_size as usize];
        let json_str = std::str::from_utf8(json_bytes)?;
        let data: Value = serde_json::from_str(json_str)?;
        
        Ok(IpcPacket {
            packet_type,
            data,
        })
    }
}

#[derive(Debug, Clone)]
pub struct IpcCloseCode;

impl IpcCloseCode {
    pub const CLOSE_NORMAL: u32 = 1000;
    pub const CLOSE_UNSUPPORTED: u32 = 1003;
    pub const CLOSE_ABNORMAL: u32 = 1006;
}

#[derive(Debug, Clone)]
pub struct IpcErrorCode;

impl IpcErrorCode {
    pub const INVALID_CLIENTID: u32 = 4000;
    pub const INVALID_ORIGIN: u32 = 4001;
    pub const RATELIMITED: u32 = 4002;
    pub const TOKEN_REVOKED: u32 = 4003;
    pub const INVALID_VERSION: u32 = 4004;
    pub const INVALID_ENCODING: u32 = 4005;
}
```

### 3. Connection Management
```rust
use tokio::sync::mpsc;
use tokio::net::UnixStream;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};

pub struct IpcConnection {
    socket_id: u32,
    client_id: String,
    version: u8,
    handshook: bool,
    stream: UnixStream,
    read_buffer: Vec<u8>,
    message_tx: mpsc::UnboundedSender<RpcMessage>,
    message_rx: mpsc::UnboundedReceiver<RpcMessage>,
}

#[derive(Debug, Clone)]
pub struct HandshakeParams {
    pub version: u8,
    pub client_id: String,
}

impl HandshakeParams {
    pub fn from_json(data: &Value) -> Result<Self, IpcError> {
        let version = data.get("v")
            .and_then(|v| v.as_u64())
            .ok_or(IpcError::MissingField("v".to_string()))?;
        
        if version > u8::MAX as u64 {
            return Err(IpcError::InvalidVersion(version));
        }
        
        let client_id = data.get("client_id")
            .and_then(|id| id.as_str())
            .unwrap_or("")
            .to_string();
        
        Ok(Self {
            version: version as u8,
            client_id,
        })
    }
    
    pub fn validate(&self, config: &IpcConfig) -> Result<(), ValidationError> {
        // Version validation
        if !config.supported_versions.contains(&self.version) {
            return Err(ValidationError::UnsupportedVersion(self.version));
        }
        
        // Client ID validation
        if self.client_id.is_empty() {
            return Err(ValidationError::MissingClientId);
        }
        
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(u8),
    
    #[error("Missing client_id")]
    MissingClientId,
}

#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    
    #[error("Invalid packet type: {0}")]
    InvalidPacketType(u32),
    
    #[error("Insufficient data in buffer")]
    InsufficientData,
    
    #[error("Missing field: {0}")]
    MissingField(String),
    
    #[error("Invalid version: {0}")]
    InvalidVersion(u64),
    
    #[error("Connection validation error: {0}")]
    Validation(#[from] ValidationError),
    
    #[error("Already handshook")]
    AlreadyHandshook,
    
    #[error("Need to handshake first")]
    NotHandshook,
    
    #[error("Connection {0} not found")]
    ConnectionNotFound(u32),
    
    #[error("Socket path not available")]
    SocketPathNotAvailable,
    
    #[error("Ping timeout")]
    PingTimeout,
    
    #[error("Message channel closed")]
    ChannelClosed,
}
```

### 4. Socket Path Discovery
```rust
use tokio::time::{timeout, Duration};
use std::fs;

impl IpcServer {
    async fn find_available_socket_path(&self) -> Result<PathBuf, IpcError> {
        for attempt in 0..self.config.max_socket_tries {
            let socket_path = PathBuf::from(format!("{}-{}", self.config.socket_base_path, attempt));
            
            if self.config.debug_mode {
                tracing::debug!("Checking socket path: {:?}", socket_path);
            }
            
            match self.test_socket_availability(&socket_path).await {
                Ok(true) => {
                    // Socket is available, clean up existing file if present
                    if socket_path.exists() {
                        if let Err(e) = fs::remove_file(&socket_path) {
                            tracing::warn!("Failed to remove existing socket file {:?}: {}", socket_path, e);
                        }
                    }
                    
                    if self.config.debug_mode {
                        tracing::debug!("Socket path available: {:?}", socket_path);
                    }
                    
                    return Ok(socket_path);
                }
                Ok(false) => {
                    if self.config.debug_mode {
                        tracing::debug!("Socket path in use: {:?}", socket_path);
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!("Error testing socket path {:?}: {}", socket_path, e);
                    continue;
                }
            }
        }
        
        Err(IpcError::SocketPathNotAvailable)
    }
    
    async fn test_socket_availability(&self, path: &PathBuf) -> Result<bool, IpcError> {
        // Try to connect to the socket to see if it's in use
        match UnixStream::connect(path).await {
            Ok(stream) => {
                // Socket exists and is accepting connections, test if it's a Discord IPC socket
                self.test_discord_ipc_socket(stream).await
            }
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        // Socket file doesn't exist, path is available
                        Ok(true)
                    }
                    std::io::ErrorKind::ConnectionRefused => {
                        // Socket file exists but no one is listening, path is available
                        Ok(true)
                    }
                    _ => {
                        // Other error, consider path unavailable
                        Ok(false)
                    }
                }
            }
        }
    }
    
    async fn test_discord_ipc_socket(&self, mut stream: UnixStream) -> Result<bool, IpcError> {
        let unique_id = {
            let mut counter = self.unique_id_counter.lock().unwrap();
            *counter += 1;
            *counter
        };
        
        // Send a ping packet to test if this is a Discord IPC socket
        let ping_packet = IpcPacket {
            packet_type: IpcPacketType::Ping,
            data: serde_json::json!(unique_id),
        };
        
        let encoded = ping_packet.encode()?;
        
        // Set up timeout for the ping test
        let ping_result = timeout(
            Duration::from_millis(self.config.ping_timeout_ms),
            async {
                // Send ping
                stream.write_all(&encoded).await?;
                
                // Try to read response
                let mut header_buf = [0u8; 8];
                stream.read_exact(&mut header_buf).await?;
                
                let packet = IpcPacket::decode(&header_buf)?;
                
                match packet.packet_type {
                    IpcPacketType::Pong => {
                        // Check if the pong response matches our ping ID
                        if let Some(response_id) = packet.data.as_u64() {
                            Ok(response_id == unique_id as u64)
                        } else {
                            Ok(false)
                        }
                    }
                    _ => Ok(false),
                }
            }
        ).await;
        
        match ping_result {
            Ok(Ok(is_discord_socket)) => {
                // If it responded correctly to our ping, it's a Discord IPC socket
                // This means the path is NOT available for us
                Ok(!is_discord_socket)
            }
            Ok(Err(_)) | Err(_) => {
                // Error or timeout means it's probably not a Discord IPC socket
                // Or the socket is not working properly, so path is available
                Ok(true)
            }
        }
    }
}
```

## Implementation Details

### 1. Server Initialization and Socket Binding
```rust
use tokio::net::UnixListener;
use tracing::{info, warn, error, debug};

impl IpcServer {
    pub async fn new(handlers: TransportHandlers) -> Result<Self, IpcError> {
        let config = IpcConfig::default();
        
        let mut server = Self {
            listener: None,
            socket_path: None,
            active_connections: Arc::new(Mutex::new(HashMap::new())),
            connection_counter: Arc::new(Mutex::new(0)),
            unique_id_counter: Arc::new(Mutex::new(0)),
            handlers,
            config,
        };
        
        // Find available socket path and bind listener
        server.bind_to_available_socket().await?;
        
        Ok(server)
    }
    
    async fn bind_to_available_socket(&mut self) -> Result<(), IpcError> {
        let socket_path = self.find_available_socket_path().await?;
        
        // Create Unix domain socket listener
        let listener = UnixListener::bind(&socket_path)?;
        
        // Set appropriate permissions (readable/writable by user only)
        let metadata = fs::metadata(&socket_path)?;
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(&socket_path, permissions)?;
        
        info!("IPC server listening at {:?}", socket_path);
        
        self.listener = Some(listener);
        self.socket_path = Some(socket_path);
        
        Ok(())
    }
    
    pub async fn start(&mut self) -> Result<(), IpcError> {
        let listener = self.listener.take()
            .ok_or_else(|| IpcError::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Server not properly initialized"
            )))?;
        
        info!("Starting IPC server on socket {:?}", 
              self.socket_path.as_ref().unwrap());
        
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    if self.config.debug_mode {
                        debug!("New IPC connection");
                    }
                    
                    let connections = self.active_connections.clone();
                    let counter = self.connection_counter.clone();
                    let handlers = self.handlers.clone();
                    let config = self.config.clone();
                    
                    // Spawn connection handler
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(
                            stream, 
                            connections, 
                            counter, 
                            handlers, 
                            config
                        ).await {
                            error!("IPC connection handling error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept IPC connection: {}", e);
                    // Continue accepting other connections
                }
            }
        }
    }
}
```

### 2. Connection Handling and Protocol Implementation
```rust
use tokio::io::{AsyncReadExt, AsyncWriteExt};

impl IpcServer {
    async fn handle_connection(
        mut stream: UnixStream,
        connections: Arc<Mutex<HashMap<u32, IpcConnection>>>,
        counter: Arc<Mutex<u32>>,
        handlers: TransportHandlers,
        config: IpcConfig,
    ) -> Result<(), IpcError> {
        // Generate unique connection ID
        let socket_id = {
            let mut c = counter.lock().unwrap();
            *c += 1;
            *c
        };
        
        if config.debug_mode {
            debug!("IPC connection established: id={}", socket_id);
        }
        
        // Create bidirectional message channel
        let (message_tx, message_rx) = mpsc::unbounded_channel();
        
        // Create connection object
        let mut connection = IpcConnection {
            socket_id,
            client_id: String::new(),
            version: 1,
            handshook: false,
            stream,
            read_buffer: Vec::with_capacity(8192),
            message_tx: message_tx.clone(),
            message_rx,
        };
        
        // Start message handling loop
        let read_connections = connections.clone();
        let write_connections = connections.clone();
        let read_handlers = handlers.clone();
        
        // Store connection
        {
            let mut conns = connections.lock().unwrap();
            conns.insert(socket_id, connection);
        }
        
        // Spawn read task
        let read_task = tokio::spawn(async move {
            Self::handle_incoming_messages(socket_id, read_connections, read_handlers, config.clone()).await
        });
        
        // Spawn write task  
        let write_task = tokio::spawn(async move {
            Self::handle_outgoing_messages(socket_id, write_connections).await
        });
        
        // Wait for either task to complete (connection closed)
        let result = tokio::select! {
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
        
        if let Err(e) = result {
            error!("Connection task error for {}: {}", socket_id, e);
        }
        
        Ok(())
    }
    
    async fn handle_incoming_messages(
        socket_id: u32,
        connections: Arc<Mutex<HashMap<u32, IpcConnection>>>,
        handlers: TransportHandlers,
        config: IpcConfig,
    ) -> Result<(), IpcError> {
        loop {
            // Read packet from socket
            let packet = Self::read_packet(socket_id, &connections).await?;
            
            if config.debug_mode {
                debug!("Received IPC packet from {}: {:?}", socket_id, packet.packet_type);
            }
            
            match packet.packet_type {
                IpcPacketType::Ping => {
                    // Respond with pong
                    let pong_packet = IpcPacket {
                        packet_type: IpcPacketType::Pong,
                        data: packet.data,
                    };
                    Self::send_packet(socket_id, &connections, pong_packet).await?;
                }
                
                IpcPacketType::Pong => {
                    // Pong received, no action needed
                    if config.debug_mode {
                        debug!("Pong received from {}", socket_id);
                    }
                }
                
                IpcPacketType::Handshake => {
                    Self::handle_handshake(socket_id, &connections, &handlers, &config, packet.data).await?;
                }
                
                IpcPacketType::Frame => {
                    Self::handle_frame(socket_id, &connections, &handlers, &config, packet.data).await?;
                }
                
                IpcPacketType::Close => {
                    info!("Close packet received from {}", socket_id);
                    return Ok(());
                }
            }
        }
    }
    
    async fn read_packet(
        socket_id: u32,
        connections: &Arc<Mutex<HashMap<u32, IpcConnection>>>,
    ) -> Result<IpcPacket, IpcError> {
        // Read 8-byte header first
        let mut header_buf = [0u8; 8];
        {
            let mut conns = connections.lock().unwrap();
            if let Some(connection) = conns.get_mut(&socket_id) {
                connection.stream.read_exact(&mut header_buf).await?;
            } else {
                return Err(IpcError::ConnectionNotFound(socket_id));
            }
        }
        
        // Parse header to get packet type and data size
        let mut cursor = Cursor::new(&header_buf);
        let packet_type_raw = cursor.read_u32::<LittleEndian>()?;
        let data_size = cursor.read_u32::<LittleEndian>()?;
        
        let packet_type = IpcPacketType::try_from(packet_type_raw)?;
        
        // Read data payload
        let mut data_buf = vec![0u8; data_size as usize];
        {
            let mut conns = connections.lock().unwrap();
            if let Some(connection) = conns.get_mut(&socket_id) {
                connection.stream.read_exact(&mut data_buf).await?;
            } else {
                return Err(IpcError::ConnectionNotFound(socket_id));
            }
        }
        
        // Parse JSON data
        let json_str = std::str::from_utf8(&data_buf)?;
        let data: Value = serde_json::from_str(json_str)?;
        
        Ok(IpcPacket {
            packet_type,
            data,
        })
    }
    
    async fn send_packet(
        socket_id: u32,
        connections: &Arc<Mutex<HashMap<u32, IpcConnection>>>,
        packet: IpcPacket,
    ) -> Result<(), IpcError> {
        let encoded = packet.encode()?;
        
        let mut conns = connections.lock().unwrap();
        if let Some(connection) = conns.get_mut(&socket_id) {
            connection.stream.write_all(&encoded).await?;
            Ok(())
        } else {
            Err(IpcError::ConnectionNotFound(socket_id))
        }
    }
}
```

### 3. Handshake and Frame Processing
```rust
impl IpcServer {
    async fn handle_handshake(
        socket_id: u32,
        connections: &Arc<Mutex<HashMap<u32, IpcConnection>>>,
        handlers: &TransportHandlers,
        config: &IpcConfig,
        data: Value,
    ) -> Result<(), IpcError> {
        // Check if already handshook
        {
            let conns = connections.lock().unwrap();
            if let Some(connection) = conns.get(&socket_id) {
                if connection.handshook {
                    return Err(IpcError::AlreadyHandshook);
                }
            } else {
                return Err(IpcError::ConnectionNotFound(socket_id));
            }
        }
        
        if config.debug_mode {
            debug!("Processing handshake from {}: {:?}", socket_id, data);
        }
        
        // Parse handshake parameters
        let params = HandshakeParams::from_json(&data)?;
        
        // Validate handshake
        if let Err(validation_error) = params.validate(config) {
            let error_code = match validation_error {
                ValidationError::UnsupportedVersion(_) => IpcErrorCode::INVALID_VERSION,
                ValidationError::MissingClientId => IpcErrorCode::INVALID_CLIENTID,
            };
            
            warn!("Handshake validation failed for {}: {}", socket_id, validation_error);
            
            Self::send_close_packet(
                socket_id,
                connections,
                error_code,
                validation_error.to_string(),
            ).await?;
            
            return Err(IpcError::Validation(validation_error));
        }
        
        // Update connection state
        {
            let mut conns = connections.lock().unwrap();
            if let Some(connection) = conns.get_mut(&socket_id) {
                connection.handshook = true;
                connection.client_id = params.client_id.clone();
                connection.version = params.version;
            } else {
                return Err(IpcError::ConnectionNotFound(socket_id));
            }
        }
        
        // Create transport connection for RPC server
        let transport_connection = SocketConnection {
            socket_id,
            client_id: params.client_id,
            transport_type: TransportType::Ipc,
            sender: {
                let conns = connections.lock().unwrap();
                conns.get(&socket_id).unwrap().message_tx.clone()
            },
        };
        
        // Notify RPC server of new connection
        (handlers.on_connection)(transport_connection);
        
        info!("IPC handshake completed for socket {}", socket_id);
        Ok(())
    }
    
    async fn handle_frame(
        socket_id: u32,
        connections: &Arc<Mutex<HashMap<u32, IpcConnection>>>,
        handlers: &TransportHandlers,
        config: &IpcConfig,
        data: Value,
    ) -> Result<(), IpcError> {
        // Check if handshake was completed
        {
            let conns = connections.lock().unwrap();
            if let Some(connection) = conns.get(&socket_id) {
                if !connection.handshook {
                    return Err(IpcError::NotHandshook);
                }
            } else {
                return Err(IpcError::ConnectionNotFound(socket_id));
            }
        }
        
        if config.debug_mode {
            debug!("Processing frame from {}: {:?}", socket_id, data);
        }
        
        // Parse RPC request from frame data
        let request: RpcRequest = serde_json::from_value(data)
            .map_err(|e| IpcError::Json(e))?;
        
        // Forward to RPC server
        (handlers.on_message)(socket_id, request);
        
        Ok(())
    }
    
    async fn send_close_packet(
        socket_id: u32,
        connections: &Arc<Mutex<HashMap<u32, IpcConnection>>>,
        code: u32,
        message: String,
    ) -> Result<(), IpcError> {
        let close_packet = IpcPacket {
            packet_type: IpcPacketType::Close,
            data: serde_json::json!({
                "code": code,
                "message": message
            }),
        };
        
        Self::send_packet(socket_id, connections, close_packet).await
    }
    
    async fn handle_outgoing_messages(
        socket_id: u32,
        connections: Arc<Mutex<HashMap<u32, IpcConnection>>>,
    ) -> Result<(), IpcError> {
        loop {
            // Get message from channel
            let message = {
                let mut conns = connections.lock().unwrap();
                if let Some(connection) = conns.get_mut(&socket_id) {
                    match connection.message_rx.recv().await {
                        Some(msg) => msg,
                        None => {
                            info!("Message channel closed for IPC connection {}", socket_id);
                            return Ok(());
                        }
                    }
                } else {
                    warn!("IPC connection {} not found for outgoing message", socket_id);
                    return Ok(());
                }
            };
            
            // Send as frame packet
            let frame_packet = IpcPacket {
                packet_type: IpcPacketType::Frame,
                data: serde_json::to_value(&message)?,
            };
            
            if let Err(e) = Self::send_packet(socket_id, &connections, frame_packet).await {
                error!("Failed to send frame to IPC connection {}: {}", socket_id, e);
                return Err(e);
            }
        }
    }
}
```

### 4. Connection Lifecycle Management
```rust
impl IpcServer {
    pub async fn shutdown(&mut self) -> Result<(), IpcError> {
        info!("Shutting down IPC server");
        
        // Close all active connections
        let connections_to_close: Vec<u32> = {
            let conns = self.active_connections.lock().unwrap();
            conns.keys().cloned().collect()
        };
        
        for socket_id in connections_to_close {
            self.close_connection(
                socket_id, 
                Some(IpcCloseCode::CLOSE_NORMAL), 
                Some("Server shutdown".to_string())
            ).await?;
        }
        
        // Clean up connections map
        {
            let mut conns = self.active_connections.lock().unwrap();
            conns.clear();
        }
        
        // Remove socket file
        if let Some(socket_path) = &self.socket_path {
            if socket_path.exists() {
                if let Err(e) = fs::remove_file(socket_path) {
                    warn!("Failed to remove socket file {:?}: {}", socket_path, e);
                } else {
                    info!("Removed socket file: {:?}", socket_path);
                }
            }
        }
        
        info!("IPC server shutdown complete");
        Ok(())
    }
    
    async fn close_connection(
        &self,
        socket_id: u32,
        close_code: Option<u32>,
        reason: Option<String>,
    ) -> Result<(), IpcError> {
        if let (Some(code), Some(message)) = (close_code, reason) {
            // Send close packet first
            let _ = Self::send_close_packet(socket_id, &self.active_connections, code, message).await;
        }
        
        // Remove and close connection
        let mut conns = self.active_connections.lock().unwrap();
        if let Some(mut connection) = conns.remove(&socket_id) {
            let _ = connection.stream.shutdown().await;
            info!("Closed IPC connection {}", socket_id);
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
    
    pub fn get_connection_info(&self, socket_id: u32) -> Option<IpcConnectionInfo> {
        let conns = self.active_connections.lock().unwrap();
        conns.get(&socket_id).map(|conn| IpcConnectionInfo {
            socket_id: conn.socket_id,
            client_id: conn.client_id.clone(),
            version: conn.version,
            handshook: conn.handshook,
        })
    }
    
    pub fn get_socket_path(&self) -> Option<&PathBuf> {
        self.socket_path.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct IpcConnectionInfo {
    pub socket_id: u32,
    pub client_id: String,
    pub version: u8,
    pub handshook: bool,
}
```

## Security Implementation

### 1. Socket Permission Management
```rust
impl IpcServer {
    fn set_socket_permissions(socket_path: &PathBuf) -> Result<(), IpcError> {
        // Set socket file permissions to be readable/writable by owner only
        let metadata = fs::metadata(socket_path)?;
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o600); // rw-------
        fs::set_permissions(socket_path, permissions)?;
        
        info!("Set socket permissions for {:?} to 0600", socket_path);
        Ok(())
    }
    
    fn validate_socket_security(socket_path: &PathBuf) -> Result<(), IpcError> {
        let metadata = fs::metadata(socket_path)?;
        let permissions = metadata.permissions();
        
        // Check that socket is not world-readable or world-writable
        let mode = permissions.mode();
        if mode & 0o044 != 0 {
            warn!("Socket {:?} has unsafe permissions: {:o}", socket_path, mode);
            return Err(IpcError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Socket has unsafe permissions"
            )));
        }
        
        Ok(())
    }
}
```

### 2. Connection Rate Limiting
```rust
use std::time::{Duration, Instant};
use std::collections::VecDeque;

pub struct IpcRateLimiter {
    window_size: Duration,
    max_connections: usize,
    connection_times: VecDeque<Instant>,
}

impl IpcRateLimiter {
    pub fn new(window_size: Duration, max_connections: usize) -> Self {
        Self {
            window_size,
            max_connections,
            connection_times: VecDeque::new(),
        }
    }
    
    pub fn check_connection_rate_limit(&mut self) -> bool {
        let now = Instant::now();
        
        // Remove old connection times outside the window
        while let Some(&front) = self.connection_times.front() {
            if now.duration_since(front) > self.window_size {
                self.connection_times.pop_front();
            } else {
                break;
            }
        }
        
        // Check if we're within the limit
        if self.connection_times.len() < self.max_connections {
            self.connection_times.push_back(now);
            true
        } else {
            false
        }
    }
}

impl IpcServer {
    async fn check_connection_rate_limit(&mut self) -> bool {
        // This would be integrated into the connection handling
        // For now, we'll use a simple global rate limiter
        true // Placeholder
    }
}
```

## Error Handling and Logging

### 1. Comprehensive Error Types
```rust
#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    
    #[error("Invalid packet type: {0}")]
    InvalidPacketType(u32),
    
    #[error("Insufficient data in buffer")]
    InsufficientData,
    
    #[error("Missing field: {0}")]
    MissingField(String),
    
    #[error("Invalid version: {0}")]
    InvalidVersion(u64),
    
    #[error("Connection validation error: {0}")]
    Validation(#[from] ValidationError),
    
    #[error("Already handshook")]
    AlreadyHandshook,
    
    #[error("Need to handshake first")]
    NotHandshook,
    
    #[error("Connection {0} not found")]
    ConnectionNotFound(u32),
    
    #[error("Socket path not available")]
    SocketPathNotAvailable,
    
    #[error("Ping timeout")]
    PingTimeout,
    
    #[error("Message channel closed")]
    ChannelClosed,
    
    #[error("Socket operation failed")]
    SocketOperation,
    
    #[error("Protocol violation: {0}")]
    ProtocolViolation(String),
}
```

### 2. Structured Logging
```rust
use tracing::{info, warn, error, debug, instrument};

impl IpcServer {
    #[instrument(skip(self), fields(socket_path = ?self.socket_path))]
    pub async fn start_with_logging(&mut self) -> Result<(), IpcError> {
        info!("Starting IPC transport");
        
        match self.start().await {
            Ok(()) => {
                info!("IPC transport started successfully");
                Ok(())
            }
            Err(e) => {
                error!("Failed to start IPC transport: {}", e);
                Err(e)
            }
        }
    }
    
    #[instrument(skip(connections, handlers), fields(socket_id))]
    async fn log_connection_event(
        socket_id: u32,
        event: &str,
        client_id: &str,
    ) {
        info!(
            socket_id = socket_id,
            client_id = client_id,
            event = event,
            "IPC connection event"
        );
    }
    
    #[instrument(skip(packet), fields(socket_id, packet_type = ?packet.packet_type))]
    fn log_packet_event(socket_id: u32, packet: &IpcPacket, direction: &str) {
        debug!(
            socket_id = socket_id,
            packet_type = ?packet.packet_type,
            has_data = !packet.data.is_null(),
            direction = direction,
            "IPC packet"
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

impl IpcServer {
    pub fn new_with_handlers(handlers: TransportHandlers) -> Result<Self, IpcError> {
        Ok(Self {
            listener: None,
            socket_path: None,
            active_connections: Arc::new(Mutex::new(HashMap::new())),
            connection_counter: Arc::new(Mutex::new(0)),
            unique_id_counter: Arc::new(Mutex::new(0)),
            handlers,
            config: IpcConfig::default(),
        })
    }
    
    // Bridge between IPC transport and RPC server
    fn create_socket_connection(
        socket_id: u32,
        params: &HandshakeParams,
        message_tx: mpsc::UnboundedSender<RpcMessage>,
    ) -> SocketConnection {
        SocketConnection {
            socket_id,
            client_id: params.client_id.clone(),
            transport_type: TransportType::Ipc,
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
    pub fn send(&self, message: RpcMessage) -> Result<(), IpcError> {
        self.sender.send(message)
            .map_err(|_| IpcError::ChannelClosed)
    }
}

#[derive(Debug, Clone)]
pub enum TransportType {
    Ipc,
    WebSocket,
    Process,
}

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
```

### 2. Message Flow Implementation
```rust
impl IpcServer {
    // Called by RPC server to send messages to IPC clients
    pub async fn send_to_client(
        &self,
        socket_id: u32,
        message: RpcMessage,
    ) -> Result<(), IpcError> {
        let connections = self.active_connections.clone();
        
        // Send message through the connection's channel
        {
            let conns = connections.lock().unwrap();
            if let Some(connection) = conns.get(&socket_id) {
                connection.message_tx.send(message)
                    .map_err(|_| IpcError::ChannelClosed)?;
            } else {
                return Err(IpcError::ConnectionNotFound(socket_id));
            }
        }
        
        Ok(())
    }
    
    // Called by RPC server to broadcast to all IPC clients
    pub async fn broadcast_to_clients(
        &self,
        message: RpcMessage,
        exclude_socket: Option<u32>,
    ) -> Result<(), IpcError> {
        let socket_ids: Vec<u32> = {
            let conns = self.active_connections.lock().unwrap();
            conns.keys()
                .filter(|&&id| Some(id) != exclude_socket)
                .cloned()
                .collect()
        };
        
        for socket_id in socket_ids {
            if let Err(e) = self.send_to_client(socket_id, message.clone()).await {
                warn!("Failed to send broadcast message to IPC {}: {}", socket_id, e);
            }
        }
        
        Ok(())
    }
    
    // Get statistics for monitoring
    pub fn get_stats(&self) -> IpcStats {
        let conns = self.active_connections.lock().unwrap();
        
        let mut client_ids = Vec::new();
        let mut handshook_count = 0;
        
        for conn in conns.values() {
            client_ids.push(conn.client_id.clone());
            if conn.handshook {
                handshook_count += 1;
            }
        }
        
        IpcStats {
            active_connections: conns.len(),
            handshook_connections: handshook_count,
            socket_path: self.socket_path.clone(),
            client_ids,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IpcStats {
    pub active_connections: usize,
    pub handshook_connections: usize,
    pub socket_path: Option<PathBuf>,
    pub client_ids: Vec<String>,
}
```

## Testing Strategy

### 1. Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;
    use tempfile::tempdir;
    
    #[test]
    async fn test_packet_encode_decode() {
        let packet = IpcPacket {
            packet_type: IpcPacketType::Handshake,
            data: serde_json::json!({
                "v": 1,
                "client_id": "123456789"
            }),
        };
        
        let encoded = packet.encode().unwrap();
        let decoded = IpcPacket::decode(&encoded).unwrap();
        
        assert_eq!(decoded.packet_type, IpcPacketType::Handshake);
        assert_eq!(decoded.data["v"], 1);
        assert_eq!(decoded.data["client_id"], "123456789");
    }
    
    #[test]
    async fn test_handshake_params_validation() {
        let valid_data = serde_json::json!({
            "v": 1,
            "client_id": "123456789"
        });
        
        let params = HandshakeParams::from_json(&valid_data).unwrap();
        let config = IpcConfig::default();
        
        assert!(params.validate(&config).is_ok());
        assert_eq!(params.version, 1);
        assert_eq!(params.client_id, "123456789");
        
        // Test invalid version
        let invalid_version = serde_json::json!({
            "v": 2,
            "client_id": "123456789"
        });
        
        let params = HandshakeParams::from_json(&invalid_version).unwrap();
        assert!(params.validate(&config).is_err());
        
        // Test missing client_id
        let missing_client = serde_json::json!({
            "v": 1,
            "client_id": ""
        });
        
        let params = HandshakeParams::from_json(&missing_client).unwrap();
        assert!(params.validate(&config).is_err());
    }
    
    #[test]
    async fn test_packet_type_conversion() {
        assert_eq!(IpcPacketType::try_from(0).unwrap(), IpcPacketType::Handshake);
        assert_eq!(IpcPacketType::try_from(1).unwrap(), IpcPacketType::Frame);
        assert_eq!(IpcPacketType::try_from(2).unwrap(), IpcPacketType::Close);
        assert_eq!(IpcPacketType::try_from(3).unwrap(), IpcPacketType::Ping);
        assert_eq!(IpcPacketType::try_from(4).unwrap(), IpcPacketType::Pong);
        
        assert!(IpcPacketType::try_from(5).is_err());
        assert!(IpcPacketType::try_from(999).is_err());
    }
    
    #[test]
    async fn test_socket_path_generation() {
        let config = IpcConfig::default();
        
        // Test that socket paths are generated correctly
        for i in 0..config.max_socket_tries {
            let expected_path = format!("{}-{}", config.socket_base_path, i);
            // This would test the path generation logic
        }
    }
}
```

### 2. Integration Tests
```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    use tokio::net::UnixStream;
    use std::sync::mpsc;
    use tempfile::tempdir;
    
    #[tokio::test]
    async fn test_ipc_connection_flow() {
        // Create temporary directory for socket
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test-discord-ipc-0");
        
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
        
        // Start IPC server with custom socket path
        let mut config = IpcConfig::default();
        config.socket_base_path = socket_path.to_string_lossy().to_string();
        
        let mut server = IpcServer::new_with_handlers(handlers).unwrap();
        server.config = config;
        
        tokio::spawn(async move {
            server.start().await.unwrap();
        });
        
        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Connect client
        let mut client_stream = UnixStream::connect(&socket_path).await.unwrap();
        
        // Send handshake
        let handshake_packet = IpcPacket {
            packet_type: IpcPacketType::Handshake,
            data: serde_json::json!({
                "v": 1,
                "client_id": "123456789"
            }),
        };
        
        let encoded = handshake_packet.encode().unwrap();
        client_stream.write_all(&encoded).await.unwrap();
        
        // Verify connection was registered
        let connection = conn_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(connection.client_id, "123456789");
        
        // Send test frame
        let frame_packet = IpcPacket {
            packet_type: IpcPacketType::Frame,
            data: serde_json::json!({
                "cmd": "SET_ACTIVITY",
                "args": {"activity": null},
                "nonce": "test-123"
            }),
        };
        
        let encoded = frame_packet.encode().unwrap();
        client_stream.write_all(&encoded).await.unwrap();
        
        // Verify message was received
        let (socket_id, request) = msg_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(request.cmd, "SET_ACTIVITY");
        assert_eq!(request.nonce, Some("test-123".to_string()));
        
        // Close connection
        let close_packet = IpcPacket {
            packet_type: IpcPacketType::Close,
            data: serde_json::json!({}),
        };
        
        let encoded = close_packet.encode().unwrap();
        client_stream.write_all(&encoded).await.unwrap();
        
        // Verify close was registered
        let closed_id = close_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(closed_id, socket_id);
    }
    
    #[tokio::test]
    async fn test_ping_pong_flow() {
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test-discord-ipc-0");
        
        let handlers = TransportHandlers {
            on_connection: Arc::new(|_| {}),
            on_message: Arc::new(|_, _| {}),
            on_close: Arc::new(|_| {}),
        };
        
        let mut config = IpcConfig::default();
        config.socket_base_path = socket_path.to_string_lossy().to_string();
        
        let mut server = IpcServer::new_with_handlers(handlers).unwrap();
        server.config = config;
        
        tokio::spawn(async move {
            server.start().await.unwrap();
        });
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let mut client_stream = UnixStream::connect(&socket_path).await.unwrap();
        
        // Send ping
        let ping_packet = IpcPacket {
            packet_type: IpcPacketType::Ping,
            data: serde_json::json!(12345),
        };
        
        let encoded = ping_packet.encode().unwrap();
        client_stream.write_all(&encoded).await.unwrap();
        
        // Read pong response
        let mut header_buf = [0u8; 8];
        client_stream.read_exact(&mut header_buf).await.unwrap();
        
        let mut cursor = Cursor::new(&header_buf);
        let packet_type = cursor.read_u32::<LittleEndian>().unwrap();
        let data_size = cursor.read_u32::<LittleEndian>().unwrap();
        
        assert_eq!(packet_type, IpcPacketType::Pong as u32);
        
        let mut data_buf = vec![0u8; data_size as usize];
        client_stream.read_exact(&mut data_buf).await.unwrap();
        
        let response_data: Value = serde_json::from_slice(&data_buf).unwrap();
        assert_eq!(response_data, 12345);
    }
    
    #[tokio::test]
    async fn test_invalid_handshake_rejection() {
        let temp_dir = tempdir().unwrap();
        let socket_path = temp_dir.path().join("test-discord-ipc-0");
        
        let handlers = TransportHandlers {
            on_connection: Arc::new(|_| {}),
            on_message: Arc::new(|_, _| {}),
            on_close: Arc::new(|_| {}),
        };
        
        let mut config = IpcConfig::default();
        config.socket_base_path = socket_path.to_string_lossy().to_string();
        
        let mut server = IpcServer::new_with_handlers(handlers).unwrap();
        server.config = config;
        
        tokio::spawn(async move {
            server.start().await.unwrap();
        });
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let mut client_stream = UnixStream::connect(&socket_path).await.unwrap();
        
        // Send invalid handshake (unsupported version)
        let invalid_handshake = IpcPacket {
            packet_type: IpcPacketType::Handshake,
            data: serde_json::json!({
                "v": 2,
                "client_id": "123456789"
            }),
        };
        
        let encoded = invalid_handshake.encode().unwrap();
        client_stream.write_all(&encoded).await.unwrap();
        
        // Should receive close packet with error code
        let mut header_buf = [0u8; 8];
        client_stream.read_exact(&mut header_buf).await.unwrap();
        
        let mut cursor = Cursor::new(&header_buf);
        let packet_type = cursor.read_u32::<LittleEndian>().unwrap();
        let data_size = cursor.read_u32::<LittleEndian>().unwrap();
        
        assert_eq!(packet_type, IpcPacketType::Close as u32);
        
        let mut data_buf = vec![0u8; data_size as usize];
        client_stream.read_exact(&mut data_buf).await.unwrap();
        
        let close_data: Value = serde_json::from_slice(&data_buf).unwrap();
        assert_eq!(close_data["code"], IpcErrorCode::INVALID_VERSION);
    }
}
```

## Performance Optimization

### 1. Connection Pooling and Buffer Management
```rust
use bytes::{BytesMut, Buf, BufMut};

pub struct OptimizedIpcConnection {
    socket_id: u32,
    client_id: String,
    version: u8,
    handshook: bool,
    stream: UnixStream,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
    message_tx: mpsc::UnboundedSender<RpcMessage>,
    message_rx: mpsc::UnboundedReceiver<RpcMessage>,
}

impl OptimizedIpcConnection {
    pub fn new(
        socket_id: u32,
        stream: UnixStream,
        message_tx: mpsc::UnboundedSender<RpcMessage>,
        message_rx: mpsc::UnboundedReceiver<RpcMessage>,
    ) -> Self {
        Self {
            socket_id,
            client_id: String::new(),
            version: 1,
            handshook: false,
            stream,
            read_buffer: BytesMut::with_capacity(8192),
            write_buffer: BytesMut::with_capacity(8192),
            message_tx,
            message_rx,
        }
    }
    
    pub async fn read_packet_optimized(&mut self) -> Result<Option<IpcPacket>, IpcError> {
        // Ensure we have at least 8 bytes for the header
        while self.read_buffer.len() < 8 {
            let mut temp_buf = [0u8; 4096];
            let n = self.stream.read(&mut temp_buf).await?;
            if n == 0 {
                return Ok(None); // Connection closed
            }
            self.read_buffer.extend_from_slice(&temp_buf[..n]);
        }
        
        // Parse header
        let packet_type_raw = (&self.read_buffer[0..4]).get_u32_le();
        let data_size = (&self.read_buffer[4..8]).get_u32_le();
        
        let packet_type = IpcPacketType::try_from(packet_type_raw)?;
        
        // Ensure we have the complete packet
        let total_size = 8 + data_size as usize;
        while self.read_buffer.len() < total_size {
            let mut temp_buf = [0u8; 4096];
            let n = self.stream.read(&mut temp_buf).await?;
            if n == 0 {
                return Err(IpcError::InsufficientData);
            }
            self.read_buffer.extend_from_slice(&temp_buf[..n]);
        }
        
        // Extract packet data
        let packet_data = self.read_buffer.split_to(total_size);
        let data_bytes = &packet_data[8..];
        let data: Value = serde_json::from_slice(data_bytes)?;
        
        Ok(Some(IpcPacket {
            packet_type,
            data,
        }))
    }
    
    pub async fn write_packet_optimized(&mut self, packet: IpcPacket) -> Result<(), IpcError> {
        let encoded = packet.encode()?;
        
        // Buffer the write
        self.write_buffer.extend_from_slice(&encoded);
        
        // Flush if buffer is getting large
        if self.write_buffer.len() > 4096 {
            self.flush_write_buffer().await?;
        }
        
        Ok(())
    }
    
    pub async fn flush_write_buffer(&mut self) -> Result<(), IpcError> {
        if !self.write_buffer.is_empty() {
            self.stream.write_all(&self.write_buffer).await?;
            self.write_buffer.clear();
        }
        Ok(())
    }
}
```

### 2. Message Batching for High Throughput
```rust
use tokio::time::{interval, Duration};

pub struct IpcMessageBatcher {
    pending_packets: Vec<(u32, IpcPacket)>,
    batch_size: usize,
    flush_interval: Duration,
}

impl IpcMessageBatcher {
    pub fn new(batch_size: usize, flush_interval: Duration) -> Self {
        Self {
            pending_packets: Vec::with_capacity(batch_size),
            batch_size,
            flush_interval,
        }
    }
    
    pub async fn add_packet(&mut self, socket_id: u32, packet: IpcPacket) -> Vec<(u32, IpcPacket)> {
        self.pending_packets.push((socket_id, packet));
        
        if self.pending_packets.len() >= self.batch_size {
            self.flush()
        } else {
            Vec::new()
        }
    }
    
    pub fn flush(&mut self) -> Vec<(u32, IpcPacket)> {
        std::mem::take(&mut self.pending_packets)
    }
    
    pub async fn start_flush_timer(&mut self, sender: mpsc::UnboundedSender<Vec<(u32, IpcPacket)>>) {
        let mut interval = interval(self.flush_interval);
        
        loop {
            interval.tick().await;
            
            if !self.pending_packets.is_empty() {
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

### 1. Health Checks and Status
```rust
#[derive(Debug, Serialize)]
pub struct IpcHealthStatus {
    pub status: String,
    pub socket_path: Option<String>,
    pub connection_count: usize,
    pub handshook_connections: usize,
    pub uptime_seconds: u64,
    pub total_connections: u64,
    pub total_messages: u64,
    pub last_error: Option<String>,
}

impl IpcServer {
    pub fn get_health_status(&self) -> IpcHealthStatus {
        let stats = self.get_stats();
        
        IpcHealthStatus {
            status: if self.socket_path.is_some() { "healthy" } else { "unhealthy" }.to_string(),
            socket_path: self.socket_path.as_ref().map(|p| p.to_string_lossy().to_string()),
            connection_count: stats.active_connections,
            handshook_connections: stats.handshook_connections,
            uptime_seconds: self.get_uptime_seconds(),
            total_connections: self.get_total_connections(),
            total_messages: self.get_total_messages(),
            last_error: None, // Would track last error
        }
    }
    
    fn get_uptime_seconds(&self) -> u64 {
        // Implementation would track server start time
        0
    }
    
    fn get_total_connections(&self) -> u64 {
        // Implementation would track total connections
        0
    }
    
    fn get_total_messages(&self) -> u64 {
        // Implementation would track total messages
        0
    }
}
```

### 2. Metrics Collection
```rust
use prometheus::{Counter, Gauge, Histogram, register_counter, register_gauge, register_histogram};

pub struct IpcMetrics {
    connections_total: Counter,
    active_connections: Gauge,
    handshakes_total: Counter,
    packets_sent: Counter,
    packets_received: Counter,
    packet_processing_duration: Histogram,
    errors_total: Counter,
    bytes_sent: Counter,
    bytes_received: Counter,
}

impl IpcMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        Ok(Self {
            connections_total: register_counter!(
                "ipc_connections_total",
                "Total number of IPC connections"
            )?,
            active_connections: register_gauge!(
                "ipc_active_connections",
                "Current number of active IPC connections"
            )?,
            handshakes_total: register_counter!(
                "ipc_handshakes_total",
                "Total number of successful handshakes"
            )?,
            packets_sent: register_counter!(
                "ipc_packets_sent_total",
                "Total number of packets sent"
            )?,
            packets_received: register_counter!(
                "ipc_packets_received_total", 
                "Total number of packets received"
            )?,
            packet_processing_duration: register_histogram!(
                "ipc_packet_processing_duration_seconds",
                "Time spent processing IPC packets"
            )?,
            errors_total: register_counter!(
                "ipc_errors_total",
                "Total number of IPC errors"
            )?,
            bytes_sent: register_counter!(
                "ipc_bytes_sent_total",
                "Total bytes sent over IPC"
            )?,
            bytes_received: register_counter!(
                "ipc_bytes_received_total",
                "Total bytes received over IPC"
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
    
    pub fn record_handshake(&self) {
        self.handshakes_total.inc();
    }
    
    pub fn record_packet_sent(&self, bytes: u64) {
        self.packets_sent.inc();
        self.bytes_sent.inc_by(bytes);
    }
    
    pub fn record_packet_received(&self, bytes: u64) {
        self.packets_received.inc();
        self.bytes_received.inc_by(bytes);
    }
    
    pub fn record_error(&self) {
        self.errors_total.inc();
    }
}
```

This comprehensive implementation plan provides all the necessary details for implementing a robust IPC Transport in Rust that handles Discord native client connections via Unix domain sockets, with proper binary protocol implementation, socket path discovery, connection management, and integration with the main RPC server system.