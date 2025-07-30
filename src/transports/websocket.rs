use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    extract::{
        ConnectInfo, Query,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};

use crate::error::ArrusError;
use crate::server::{RpcMessage, RpcRequest, SocketConnection, TransportHandlers, TransportType};

const PORT_RANGE: (u16, u16) = (6463, 6472);
const BIND_ADDRESS: &str = "127.0.0.1";

#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    pub port_range: (u16, u16),
    pub bind_address: String,
    pub debug_mode: bool,
    pub allowed_origins: Vec<String>,
    pub supported_versions: Vec<u8>,
    pub supported_encodings: Vec<String>,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            port_range: PORT_RANGE,
            bind_address: BIND_ADDRESS.to_string(),
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

#[derive(Debug, Deserialize)]
pub struct ConnectionParams {
    #[serde(default = "default_version")]
    pub v: u8,
    #[serde(default = "default_encoding")]
    pub encoding: String,
    #[serde(default)]
    pub client_id: String,
}

fn default_version() -> u8 {
    1
}

fn default_encoding() -> String {
    "json".to_string()
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(u8),
    #[error("Unsupported encoding: {0}")]
    UnsupportedEncoding(String),
    #[error("Disallowed origin: {0}")]
    DisallowedOrigin(String),
}

impl ConnectionParams {
    pub fn validate(&self, config: &WebSocketConfig) -> Result<(), ValidationError> {
        if !config.supported_versions.contains(&self.v) {
            return Err(ValidationError::UnsupportedVersion(self.v));
        }

        if !config.supported_encodings.contains(&self.encoding) {
            return Err(ValidationError::UnsupportedEncoding(self.encoding.clone()));
        }

        Ok(())
    }
}

#[derive(Clone)]
struct ConnectionState {
    socket_id: u32,
    client_id: String,
    message_tx: mpsc::UnboundedSender<RpcMessage>,
}

#[derive(Clone)]
struct AppState {
    handlers: TransportHandlers,
    connections: Arc<Mutex<HashMap<u32, ConnectionState>>>,
    socket_counter: Arc<Mutex<u32>>,
    config: WebSocketConfig,
}

pub struct WebSocketTransport {
    config: WebSocketConfig,
    active_port: Option<u16>,
}

impl WebSocketTransport {
    pub fn new() -> Self {
        Self {
            config: WebSocketConfig::default(),
            active_port: None,
        }
    }

    pub async fn start(&mut self, handlers: TransportHandlers) -> Result<(), ArrusError> {
        let state = AppState {
            handlers,
            connections: Arc::new(Mutex::new(HashMap::new())),
            socket_counter: Arc::new(Mutex::new(0)),
            config: self.config.clone(),
        };

        let app = Router::new()
            .route("/", get(websocket_handler))
            .with_state(state);

        // Try to bind to first available port
        for port in self.config.port_range.0..=self.config.port_range.1 {
            let addr = SocketAddr::from(([127, 0, 0, 1], port));

            if self.config.debug_mode {
                println!("Trying to bind WebSocket server to port {}", port);
            }

            match TcpListener::bind(&addr).await {
                Ok(listener) => {
                    println!("WebSocket server listening on {}", addr);
                    self.active_port = Some(port);

                    axum::serve(
                        listener,
                        app.into_make_service_with_connect_info::<SocketAddr>(),
                    )
                    .await
                    .map_err(|e| ArrusError::IoError(std::io::Error::other(e)))?;

                    return Ok(());
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::AddrInUse {
                        if self.config.debug_mode {
                            println!("Port {} is in use, trying next port", port);
                        }
                        continue;
                    } else {
                        return Err(ArrusError::IoError(std::io::Error::other(format!(
                            "Failed to bind to {}: {}",
                            addr, e
                        ))));
                    }
                }
            }
        }

        Err(ArrusError::IoError(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            format!(
                "No available ports in range {}-{}",
                self.config.port_range.0, self.config.port_range.1
            ),
        )))
    }

    pub fn get_active_port(&self) -> Option<u16> {
        self.active_port
    }
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<ConnectionParams>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Response, StatusCode> {
    // Validate connection parameters
    if let Err(e) = params.validate(&state.config) {
        if state.config.debug_mode {
            println!("Connection validation failed: {}", e);
        }
        return Err(StatusCode::BAD_REQUEST);
    }

    // Validate origin if present
    if let Some(origin) = headers.get("origin") {
        if let Ok(origin_str) = origin.to_str() {
            if !origin_str.is_empty()
                && !state
                    .config
                    .allowed_origins
                    .contains(&origin_str.to_string())
            {
                if state.config.debug_mode {
                    println!("Disallowed origin: {}", origin_str);
                }
                return Err(StatusCode::FORBIDDEN);
            }
        }
    }

    if state.config.debug_mode {
        println!(
            "New WebSocket connection from {}: client_id={}, version={}, encoding={}",
            addr, params.client_id, params.v, params.encoding
        );
    }

    Ok(ws.on_upgrade(move |socket| handle_websocket(socket, params, state)))
}

async fn handle_websocket(socket: WebSocket, params: ConnectionParams, state: AppState) {
    // Generate unique socket ID
    let socket_id = {
        let mut counter = state.socket_counter.lock().await;
        *counter += 1;
        *counter
    };

    // Create message channel for outbound messages
    let (message_tx, mut message_rx) = mpsc::unbounded_channel::<RpcMessage>();

    // Store connection state
    let connection_state = ConnectionState {
        socket_id,
        client_id: params.client_id.clone(),
        message_tx: message_tx.clone(),
    };

    {
        let mut connections = state.connections.lock().await;
        connections.insert(socket_id, connection_state);
    }

    // Create transport connection for RPC server
    let transport_connection = SocketConnection {
        socket_id,
        client_id: params.client_id.clone(),
        transport_type: TransportType::WebSocket,
        sender: message_tx,
    };

    // Notify RPC server of new connection
    (state.handlers.on_connection)(transport_connection);

    // Split the socket for concurrent reading and writing
    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Handle outbound messages
    let outbound_task = {
        let config = state.config.clone();
        tokio::spawn(async move {
            while let Some(message) = message_rx.recv().await {
                if config.debug_mode {
                    println!("Sending message to {}: {:?}", socket_id, message);
                }

                match serde_json::to_string(&message) {
                    Ok(json) => {
                        if let Err(e) = ws_sender.send(Message::Text(json)).await {
                            if config.debug_mode {
                                println!("Failed to send message to {}: {}", socket_id, e);
                            }
                            break;
                        }
                    }
                    Err(e) => {
                        if config.debug_mode {
                            println!("Failed to serialize message: {}", e);
                        }
                        break;
                    }
                }
            }
        })
    };

    // Handle inbound messages
    let inbound_task = {
        let handlers = state.handlers.clone();
        let config = state.config.clone();
        tokio::spawn(async move {
            while let Some(msg) = ws_receiver.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if config.debug_mode {
                            println!("Received message from {}: {}", socket_id, text);
                        }

                        match serde_json::from_str::<RpcRequest>(&text) {
                            Ok(request) => {
                                (handlers.on_message)(socket_id, request);
                            }
                            Err(e) => {
                                if config.debug_mode {
                                    println!(
                                        "Failed to parse RPC message from {}: {}",
                                        socket_id, e
                                    );
                                }
                                // Could send error response here
                            }
                        }
                    }
                    Ok(Message::Binary(_)) => {
                        if config.debug_mode {
                            println!("Binary message received from {}, not supported", socket_id);
                        }
                    }
                    Ok(Message::Close(frame)) => {
                        if config.debug_mode {
                            println!(
                                "WebSocket close frame received from {}: {:?}",
                                socket_id, frame
                            );
                        }
                        break;
                    }
                    Ok(Message::Ping(_)) => {
                        // Axum handles ping/pong automatically
                    }
                    Ok(Message::Pong(_)) => {
                        // Axum handles ping/pong automatically
                    }
                    Err(e) => {
                        if config.debug_mode {
                            println!("WebSocket error for connection {}: {}", socket_id, e);
                        }
                        break;
                    }
                }
            }
        })
    };

    // Wait for either task to complete (connection closed)
    tokio::select! {
        _ = outbound_task => {},
        _ = inbound_task => {},
    }

    // Clean up connection
    {
        let mut connections = state.connections.lock().await;
        connections.remove(&socket_id);
    }

    // Notify RPC server of disconnection
    (state.handlers.on_close)(socket_id);

    if state.config.debug_mode {
        println!("WebSocket connection {} closed", socket_id);
    }
}
