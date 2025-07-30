use crate::error::ArrusError;
use crate::server::{RpcMessage, RpcRequest, SocketConnection, TransportHandlers, TransportType};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// IPC packet types following Discord's binary protocol
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
    type Error = ArrusError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(IpcPacketType::Handshake),
            1 => Ok(IpcPacketType::Frame),
            2 => Ok(IpcPacketType::Close),
            3 => Ok(IpcPacketType::Ping),
            4 => Ok(IpcPacketType::Pong),
            _ => Err(ArrusError::InvalidPacketType(value)),
        }
    }
}

/// IPC packet structure
#[derive(Debug, Clone)]
pub struct IpcPacket {
    pub packet_type: IpcPacketType,
    pub data: Value,
}

impl IpcPacket {
    /// Encode packet to binary format
    pub fn encode(&self) -> Result<Vec<u8>, ArrusError> {
        let json_data = serde_json::to_string(&self.data)?;
        let data_bytes = json_data.as_bytes();
        let data_size = data_bytes.len() as u32;

        let mut buffer = Vec::with_capacity(8 + data_bytes.len());

        // Write packet type (4 bytes, little endian)
        WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, self.packet_type as u32)?;

        // Write data size (4 bytes, little endian)
        WriteBytesExt::write_u32::<LittleEndian>(&mut buffer, data_size)?;

        // Write JSON data
        buffer.extend_from_slice(data_bytes);

        Ok(buffer)
    }

    /// Decode packet from binary format
    pub fn decode(buffer: &[u8]) -> Result<Self, ArrusError> {
        if buffer.len() < 8 {
            return Err(ArrusError::InsufficientData);
        }

        let mut cursor = Cursor::new(buffer);

        // Read packet type (4 bytes, little endian)
        let packet_type_raw = ReadBytesExt::read_u32::<LittleEndian>(&mut cursor)?;
        let packet_type = IpcPacketType::try_from(packet_type_raw)?;

        // Read data size (4 bytes, little endian)
        let data_size = ReadBytesExt::read_u32::<LittleEndian>(&mut cursor)?;

        // Validate buffer has enough data
        let remaining_data = &buffer[8..];
        if remaining_data.len() < data_size as usize {
            return Err(ArrusError::InsufficientData);
        }

        // Extract and parse JSON data
        let json_bytes = &remaining_data[..data_size as usize];
        let json_str = std::str::from_utf8(json_bytes)?;
        let data: Value = serde_json::from_str(json_str)?;

        Ok(IpcPacket { packet_type, data })
    }
}

/// IPC close codes
pub struct IpcCloseCode;

impl IpcCloseCode {
    pub const CLOSE_NORMAL: u32 = 1000;
    pub const CLOSE_UNSUPPORTED: u32 = 1003;
    pub const CLOSE_ABNORMAL: u32 = 1006;
}

/// IPC error codes
pub struct IpcErrorCode;

impl IpcErrorCode {
    pub const INVALID_CLIENTID: u32 = 4000;
    pub const INVALID_ORIGIN: u32 = 4001;
    pub const RATELIMITED: u32 = 4002;
    pub const TOKEN_REVOKED: u32 = 4003;
    pub const INVALID_VERSION: u32 = 4004;
    pub const INVALID_ENCODING: u32 = 4005;
}

/// IPC server configuration
#[derive(Debug, Clone)]
pub struct IpcConfig {
    pub socket_base_path: String,
    pub max_socket_tries: u8,
    pub debug_mode: bool,
    pub supported_versions: Vec<u8>,
    pub connection_timeout_ms: u64,
    pub ping_timeout_ms: u64,
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

/// Connection information for tracking
#[derive(Debug, Clone)]
pub struct IpcConnection {
    pub socket_id: u32,
    pub client_id: String,
    pub version: u8,
    pub handshook: bool,
    pub message_tx: mpsc::UnboundedSender<RpcMessage>,
}

/// Handshake parameters
#[derive(Debug, Clone)]
pub struct HandshakeParams {
    pub version: u8,
    pub client_id: String,
}

impl HandshakeParams {
    pub fn from_json(data: &Value) -> Result<Self, ArrusError> {
        let version = data
            .get("v")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| ArrusError::MissingField("v".to_string()))?;

        if version > u8::MAX as u64 {
            return Err(ArrusError::InvalidVersion(version));
        }

        let client_id = data
            .get("client_id")
            .and_then(|id| id.as_str())
            .unwrap_or("")
            .to_string();

        Ok(Self {
            version: version as u8,
            client_id,
        })
    }

    pub fn validate(&self, config: &IpcConfig) -> Result<(), ArrusError> {
        // Version validation
        if !config.supported_versions.contains(&self.version) {
            return Err(ArrusError::UnsupportedVersion(self.version));
        }

        // Client ID validation
        if self.client_id.is_empty() {
            return Err(ArrusError::MissingClientId);
        }

        Ok(())
    }
}

/// IPC Server implementation
pub struct IpcServer {
    listener: Option<UnixListener>,
    socket_path: Option<PathBuf>,
    active_connections: Arc<Mutex<HashMap<u32, IpcConnection>>>,
    connection_counter: Arc<Mutex<u32>>,
    unique_id_counter: Arc<Mutex<u32>>,
    handlers: TransportHandlers,
    config: IpcConfig,
}

impl IpcServer {
    pub async fn new(handlers: TransportHandlers) -> Result<Self, ArrusError> {
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

    async fn bind_to_available_socket(&mut self) -> Result<(), ArrusError> {
        let socket_path = self.find_available_socket_path().await?;

        // Create Unix domain socket listener
        let listener = UnixListener::bind(&socket_path)?;

        // Set appropriate permissions (readable/writable by user only)
        #[cfg(unix)]
        {
            use std::fs;
            use std::os::unix::fs::PermissionsExt;

            let metadata = fs::metadata(&socket_path)?;
            let mut permissions = metadata.permissions();
            permissions.set_mode(0o600);
            fs::set_permissions(&socket_path, permissions)?;
        }

        info!("IPC server listening at {:?}", socket_path);

        self.listener = Some(listener);
        self.socket_path = Some(socket_path);

        Ok(())
    }

    async fn find_available_socket_path(&self) -> Result<PathBuf, ArrusError> {
        for attempt in 0..self.config.max_socket_tries {
            let socket_path =
                PathBuf::from(format!("{}-{}", self.config.socket_base_path, attempt));

            if self.config.debug_mode {
                debug!("Checking socket path: {:?}", socket_path);
            }

            match self.test_socket_availability(&socket_path).await {
                Ok(true) => {
                    // Socket is available, clean up existing file if present
                    if socket_path.exists() {
                        if let Err(e) = std::fs::remove_file(&socket_path) {
                            warn!(
                                "Failed to remove existing socket file {:?}: {}",
                                socket_path, e
                            );
                        }
                    }

                    if self.config.debug_mode {
                        debug!("Socket path available: {:?}", socket_path);
                    }

                    return Ok(socket_path);
                }
                Ok(false) => {
                    if self.config.debug_mode {
                        debug!("Socket path in use: {:?}", socket_path);
                    }
                    continue;
                }
                Err(e) => {
                    warn!("Error testing socket path {:?}: {}", socket_path, e);
                    continue;
                }
            }
        }

        Err(ArrusError::SocketPathNotAvailable)
    }

    async fn test_socket_availability(&self, path: &PathBuf) -> Result<bool, ArrusError> {
        // Try to connect to the socket to see if it's in use
        match UnixStream::connect(path).await {
            Ok(stream) => {
                // Socket exists and is accepting connections, test if it's a Discord IPC socket
                self.test_discord_ipc_socket(stream).await
            }
            Err(e) => match e.kind() {
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
            },
        }
    }

    async fn test_discord_ipc_socket(&self, mut stream: UnixStream) -> Result<bool, ArrusError> {
        use tokio::time::{Duration, timeout};

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
        let ping_result = timeout(Duration::from_millis(self.config.ping_timeout_ms), async {
            // Send ping
            stream.write_all(&encoded).await?;

            // Try to read response header
            let mut header_buf = [0u8; 8];
            stream.read_exact(&mut header_buf).await?;

            let mut cursor = Cursor::new(&header_buf);
            let packet_type_raw = ReadBytesExt::read_u32::<LittleEndian>(&mut cursor)?;
            let data_size = ReadBytesExt::read_u32::<LittleEndian>(&mut cursor)?;

            let packet_type = IpcPacketType::try_from(packet_type_raw)?;

            // Read data if it's a pong
            if packet_type == IpcPacketType::Pong {
                let mut data_buf = vec![0u8; data_size as usize];
                stream.read_exact(&mut data_buf).await?;

                let data: Value = serde_json::from_slice(&data_buf)?;
                if let Some(response_id) = data.as_u64() {
                    Ok::<bool, ArrusError>(response_id == unique_id as u64)
                } else {
                    Ok::<bool, ArrusError>(false)
                }
            } else {
                Ok::<bool, ArrusError>(false)
            }
        })
        .await;

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

    pub async fn start(&mut self) -> Result<(), ArrusError> {
        let listener = self
            .listener
            .take()
            .ok_or_else(|| ArrusError::Io("Server not properly initialized".to_string()))?;

        info!(
            "Starting IPC server on socket {:?}",
            self.socket_path.as_ref().unwrap()
        );

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
                        if let Err(e) =
                            Self::handle_connection(stream, connections, counter, handlers, config)
                                .await
                        {
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

    async fn handle_connection(
        stream: UnixStream,
        connections: Arc<Mutex<HashMap<u32, IpcConnection>>>,
        counter: Arc<Mutex<u32>>,
        handlers: TransportHandlers,
        config: IpcConfig,
    ) -> Result<(), ArrusError> {
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
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();

        // Split the stream for concurrent read/write
        let (mut read_stream, mut write_stream) = stream.into_split();

        // Spawn read task
        let read_connections = connections.clone();
        let read_handlers = handlers.clone();
        let read_config = config.clone();
        let read_task = tokio::spawn(async move {
            Self::handle_incoming_messages(
                socket_id,
                &mut read_stream,
                read_connections,
                read_handlers,
                read_config,
                message_tx,
            )
            .await
        });

        // Spawn write task
        let write_task = tokio::spawn(async move {
            Self::handle_outgoing_messages(socket_id, &mut write_stream, &mut message_rx).await
        });

        // Wait for either task to complete (connection closed)
        let result = tokio::select! {
            result = read_task => result.unwrap_or_else(|e| Err(ArrusError::Io(e.to_string()))),
            result = write_task => result.unwrap_or_else(|e| Err(ArrusError::Io(e.to_string()))),
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
        stream: &mut tokio::net::unix::OwnedReadHalf,
        connections: Arc<Mutex<HashMap<u32, IpcConnection>>>,
        handlers: TransportHandlers,
        config: IpcConfig,
        message_tx: mpsc::UnboundedSender<RpcMessage>,
    ) -> Result<(), ArrusError> {
        let mut handshook = false;

        loop {
            // Read packet from socket
            let packet = Self::read_packet(stream).await?;

            if config.debug_mode {
                debug!(
                    "Received IPC packet from {}: {:?}",
                    socket_id, packet.packet_type
                );
            }

            match packet.packet_type {
                IpcPacketType::Ping => {
                    // Respond with pong using write stream - we need to send this back through a channel
                    let _pong_packet = IpcPacket {
                        packet_type: IpcPacketType::Pong,
                        data: packet.data,
                    };
                    // For now, we'll skip ping/pong since we need the write stream
                    // This would need to be handled differently in a real implementation
                }

                IpcPacketType::Pong => {
                    // Pong received, no action needed
                    if config.debug_mode {
                        debug!("Pong received from {}", socket_id);
                    }
                }

                IpcPacketType::Handshake => {
                    if handshook {
                        return Err(ArrusError::Io("Already handshook".to_string()));
                    }

                    Self::handle_handshake(
                        socket_id,
                        &connections,
                        &handlers,
                        &config,
                        packet.data,
                        message_tx.clone(),
                    )
                    .await?;
                    handshook = true;
                }

                IpcPacketType::Frame => {
                    if !handshook {
                        return Err(ArrusError::Io("Need to handshake first".to_string()));
                    }

                    Self::handle_frame(socket_id, &handlers, &config, packet.data).await?;
                }

                IpcPacketType::Close => {
                    info!("Close packet received from {}", socket_id);
                    return Ok(());
                }
            }
        }
    }

    async fn read_packet(
        stream: &mut tokio::net::unix::OwnedReadHalf,
    ) -> Result<IpcPacket, ArrusError> {
        // Read 8-byte header first
        let mut header_buf = [0u8; 8];
        stream.read_exact(&mut header_buf).await?;

        // Parse header to get packet type and data size
        let mut cursor = Cursor::new(&header_buf);
        let packet_type_raw = ReadBytesExt::read_u32::<LittleEndian>(&mut cursor)?;
        let data_size = ReadBytesExt::read_u32::<LittleEndian>(&mut cursor)?;

        let packet_type = IpcPacketType::try_from(packet_type_raw)?;

        // Read data payload
        let mut data_buf = vec![0u8; data_size as usize];
        stream.read_exact(&mut data_buf).await?;

        // Parse JSON data
        let json_str = std::str::from_utf8(&data_buf)?;
        let data: Value = serde_json::from_str(json_str)?;

        Ok(IpcPacket { packet_type, data })
    }

    async fn handle_handshake(
        socket_id: u32,
        connections: &Arc<Mutex<HashMap<u32, IpcConnection>>>,
        handlers: &TransportHandlers,
        config: &IpcConfig,
        data: Value,
        message_tx: mpsc::UnboundedSender<RpcMessage>,
    ) -> Result<(), ArrusError> {
        if config.debug_mode {
            debug!("Processing handshake from {}: {:?}", socket_id, data);
        }

        // Parse handshake parameters
        let params = HandshakeParams::from_json(&data)?;

        // Validate handshake
        if let Err(validation_error) = params.validate(config) {
            warn!(
                "Handshake validation failed for {}: {}",
                socket_id, validation_error
            );
            return Err(validation_error);
        }

        // Create connection object
        let connection = IpcConnection {
            socket_id,
            client_id: params.client_id.clone(),
            version: params.version,
            handshook: true,
            message_tx: message_tx.clone(),
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
            transport_type: TransportType::Ipc,
            sender: message_tx,
        };

        // Notify RPC server of new connection
        (handlers.on_connection)(transport_connection);

        info!("IPC handshake completed for socket {}", socket_id);
        Ok(())
    }

    async fn handle_frame(
        socket_id: u32,
        handlers: &TransportHandlers,
        config: &IpcConfig,
        data: Value,
    ) -> Result<(), ArrusError> {
        if config.debug_mode {
            debug!("Processing frame from {}: {:?}", socket_id, data);
        }

        // Parse RPC request from frame data
        let request: RpcRequest = serde_json::from_value(data)?;

        // Forward to RPC server
        (handlers.on_message)(socket_id, request);

        Ok(())
    }

    async fn handle_outgoing_messages(
        socket_id: u32,
        stream: &mut tokio::net::unix::OwnedWriteHalf,
        message_rx: &mut mpsc::UnboundedReceiver<RpcMessage>,
    ) -> Result<(), ArrusError> {
        loop {
            match message_rx.recv().await {
                Some(message) => {
                    // Send as frame packet
                    let frame_packet = IpcPacket {
                        packet_type: IpcPacketType::Frame,
                        data: serde_json::to_value(&message)?,
                    };

                    let encoded = frame_packet.encode()?;
                    if let Err(e) = stream.write_all(&encoded).await {
                        error!(
                            "Failed to send frame to IPC connection {}: {}",
                            socket_id, e
                        );
                        return Err(ArrusError::Io(e.to_string()));
                    }
                }
                None => {
                    info!("Message channel closed for IPC connection {}", socket_id);
                    return Ok(());
                }
            }
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), ArrusError> {
        info!("Shutting down IPC server");

        // Clean up connections map
        {
            let mut conns = self.active_connections.lock().unwrap();
            conns.clear();
        }

        // Remove socket file
        if let Some(socket_path) = &self.socket_path {
            if socket_path.exists() {
                if let Err(e) = std::fs::remove_file(socket_path) {
                    warn!("Failed to remove socket file {:?}: {}", socket_path, e);
                } else {
                    info!("Removed socket file: {:?}", socket_path);
                }
            }
        }

        info!("IPC server shutdown complete");
        Ok(())
    }

    pub fn get_socket_path(&self) -> Option<&PathBuf> {
        self.socket_path.as_ref()
    }
}
