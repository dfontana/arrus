# RPC Server Implementation Plan

## Overview

The RPC Server is the central coordination layer of arRPC that manages connections from Discord clients, handles RPC messages, and coordinates between different transport layers (IPC, WebSocket) and the process scanner. It acts as the main orchestrator that maintains Discord protocol compatibility while providing Rich Presence functionality.

## Architecture Overview

The RPC Server follows an event-driven architecture using an EventEmitter pattern to coordinate between:
- **IPC Transport**: Unix domain sockets for local Discord client connections
- **WebSocket Transport**: WebSocket server for web-based Discord clients  
- **Process Server**: Automatic game detection and activity generation
- **Bridge Server**: External communication for activity forwarding

## Core Data Structures

### 1. RPC Server State
```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub struct RpcServer {
    // Core components
    ipc_server: Option<IpcServer>,
    ws_server: Option<WsServer>,
    process_server: Option<ProcessServer>,
    
    // Connection management
    socket_counter: Arc<Mutex<u32>>,
    active_sockets: Arc<Mutex<HashMap<u32, SocketInfo>>>,
    
    // Event system
    event_tx: mpsc::UnboundedSender<RpcEvent>,
    event_rx: mpsc::UnboundedReceiver<RpcEvent>,
    
    // Configuration
    config: RpcConfig,
}

pub struct SocketInfo {
    socket_id: u32,
    client_id: String,
    last_pid: Option<u32>,
    transport_type: TransportType,
    sender: mpsc::UnboundedSender<RpcMessage>,
}

pub struct RpcConfig {
    disable_process_scanning: bool,
    debug_mode: bool,
}

#[derive(Clone, Debug)]
pub enum TransportType {
    Ipc,
    WebSocket,
    Process,
}
```

### 2. Message Types and Protocol
```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

// Specific message types
#[derive(Debug, Clone)]
pub enum RpcCommand {
    SetActivity(SetActivityArgs),
    ConnectionsCallback,
    GuildTemplateBrowser(BrowserArgs),
    InviteBrowser(BrowserArgs),
    DeepLink(DeepLinkArgs),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetActivityArgs {
    pub activity: Option<Activity>,
    pub pid: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Activity {
    pub application_id: Option<String>,
    pub name: Option<String>,
    pub details: Option<String>,
    pub state: Option<String>,
    pub timestamps: Option<Timestamps>,
    pub assets: Option<Assets>,
    pub party: Option<Party>,
    pub secrets: Option<Secrets>,
    pub buttons: Option<Vec<Button>>,
    pub instance: Option<bool>,
    #[serde(rename = "type")]
    pub activity_type: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timestamps {
    pub start: Option<u64>,
    pub end: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Button {
    pub label: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserArgs {
    pub code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeepLinkArgs {
    pub params: String,
}
```

### 3. Event System
```rust
#[derive(Debug, Clone)]
pub enum RpcEvent {
    // Connection events
    Connection { socket_id: u32, socket_info: SocketInfo },
    Disconnection { socket_id: u32 },
    
    // Message events
    Message { socket_id: u32, request: RpcRequest },
    
    // Activity events
    Activity { 
        activity: Option<ProcessedActivity>, 
        pid: Option<u32>, 
        socket_id: String 
    },
    
    // Browser events
    Invite { code: String, callback: InviteCallback },
    GuildTemplate { code: String, callback: TemplateCallback },
    
    // Deep link events
    DeepLink { params: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedActivity {
    pub application_id: String,
    pub name: String,
    pub details: Option<String>,
    pub state: Option<String>,
    pub timestamps: Option<Timestamps>,
    pub assets: Option<Assets>,
    pub party: Option<Party>,
    pub secrets: Option<Secrets>,
    pub metadata: ActivityMetadata,
    pub flags: u32,
    pub buttons: Option<Vec<String>>, // Just labels for the response
    #[serde(rename = "type")]
    pub activity_type: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityMetadata {
    pub button_urls: Option<Vec<String>>,
}

pub type InviteCallback = Arc<dyn Fn(bool) + Send + Sync>;
pub type TemplateCallback = Arc<dyn Fn(bool) + Send + Sync>;
```

## Implementation Details

### 1. Server Initialization
```rust
impl RpcServer {
    pub async fn new() -> Result<Self, RpcError> {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        
        let config = RpcConfig {
            disable_process_scanning: std::env::args()
                .any(|arg| arg == "--no-process-scanning") ||
                std::env::var("ARRPC_NO_PROCESS_SCANNING").is_ok(),
            debug_mode: std::env::var("ARRPC_DEBUG").is_ok(),
        };
        
        let mut server = RpcServer {
            ipc_server: None,
            ws_server: None,
            process_server: None,
            socket_counter: Arc::new(Mutex::new(0)),
            active_sockets: Arc::new(Mutex::new(HashMap::new())),
            event_tx: event_tx.clone(),
            event_rx,
            config,
        };
        
        // Initialize transport handlers
        let handlers = TransportHandlers {
            on_connection: server.create_connection_handler(),
            on_message: server.create_message_handler(),
            on_close: server.create_close_handler(),
        };
        
        // Start IPC server
        server.ipc_server = Some(IpcServer::new(handlers.clone()).await?);
        
        // Start WebSocket server
        server.ws_server = Some(WsServer::new(handlers.clone()).await?);
        
        // Start process server (if enabled)
        if !config.disable_process_scanning {
            server.process_server = Some(ProcessServer::new(handlers).await?);
        }
        
        Ok(server)
    }
}
```

### 2. Connection Handling
```rust
impl RpcServer {
    fn create_connection_handler(&self) -> impl Fn(SocketConnection) + Send + Sync + 'static {
        let event_tx = self.event_tx.clone();
        let socket_counter = self.socket_counter.clone();
        let active_sockets = self.active_sockets.clone();
        
        move |mut socket| {
            // Generate unique socket ID
            let socket_id = {
                let mut counter = socket_counter.lock().unwrap();
                *counter += 1;
                *counter
            };
            
            // Send Discord READY event
            let ready_message = RpcMessage {
                cmd: "DISPATCH".to_string(),
                data: Some(serde_json::json!({
                    "v": 1,
                    "config": {
                        "cdn_host": "cdn.discordapp.com",
                        "api_endpoint": "//discord.com/api",
                        "environment": "production"
                    },
                    "user": {
                        "id": "1045800378228281345",
                        "username": "arrpc",
                        "discriminator": "0",
                        "global_name": "arRPC",
                        "avatar": "cfefa4d9839fb4bdf030f91c2a13e95c",
                        "avatar_decoration_data": null,
                        "bot": false,
                        "flags": 0,
                        "premium_type": 0
                    }
                })),
                evt: Some("READY".to_string()),
                nonce: None,
            };
            
            // Send ready message
            if let Err(e) = socket.send(ready_message) {
                eprintln!("Failed to send READY message: {}", e);
                return;
            }
            
            // Store socket info
            let socket_info = SocketInfo {
                socket_id,
                client_id: socket.client_id.clone(),
                last_pid: None,
                transport_type: socket.transport_type.clone(),
                sender: socket.sender.clone(),
            };
            
            {
                let mut sockets = active_sockets.lock().unwrap();
                sockets.insert(socket_id, socket_info.clone());
            }
            
            // Emit connection event
            let _ = event_tx.send(RpcEvent::Connection { socket_id, socket_info });
        }
    }
    
    fn create_close_handler(&self) -> impl Fn(u32) + Send + Sync + 'static {
        let event_tx = self.event_tx.clone();
        let active_sockets = self.active_sockets.clone();
        
        move |socket_id| {
            // Get socket info before removal
            let socket_info = {
                let mut sockets = active_sockets.lock().unwrap();
                sockets.remove(&socket_id)
            };
            
            if let Some(info) = socket_info {
                // Emit activity clear event
                let _ = event_tx.send(RpcEvent::Activity {
                    activity: None,
                    pid: info.last_pid,
                    socket_id: socket_id.to_string(),
                });
                
                // Emit disconnection event
                let _ = event_tx.send(RpcEvent::Disconnection { socket_id });
            }
        }
    }
}
```

### 3. Message Processing
```rust
impl RpcServer {
    fn create_message_handler(&self) -> impl Fn(u32, RpcRequest) + Send + Sync + 'static {
        let event_tx = self.event_tx.clone();
        let active_sockets = self.active_sockets.clone();
        
        move |socket_id, request| {
            // Emit message event first
            let _ = event_tx.send(RpcEvent::Message { 
                socket_id, 
                request: request.clone() 
            });
            
            // Process specific commands
            if let Err(e) = Self::process_command(
                socket_id, 
                request, 
                &active_sockets, 
                &event_tx
            ) {
                eprintln!("Error processing command: {}", e);
            }
        }
    }
    
    fn process_command(
        socket_id: u32,
        request: RpcRequest,
        active_sockets: &Arc<Mutex<HashMap<u32, SocketInfo>>>,
        event_tx: &mpsc::UnboundedSender<RpcEvent>,
    ) -> Result<(), RpcError> {
        let socket_info = {
            active_sockets.lock().unwrap()
                .get(&socket_id)
                .cloned()
        };
        
        let Some(socket_info) = socket_info else {
            return Err(RpcError::SocketNotFound(socket_id));
        };
        
        match request.cmd.as_str() {
            "CONNECTIONS_CALLBACK" => {
                Self::handle_connections_callback(socket_info, request.nonce)?;
            }
            
            "SET_ACTIVITY" => {
                Self::handle_set_activity(
                    socket_id, 
                    socket_info, 
                    request.args, 
                    request.nonce,
                    active_sockets,
                    event_tx
                )?;
            }
            
            "GUILD_TEMPLATE_BROWSER" => {
                Self::handle_guild_template_browser(
                    socket_info, 
                    request.args, 
                    request.nonce,
                    event_tx
                )?;
            }
            
            "INVITE_BROWSER" => {
                Self::handle_invite_browser(
                    socket_info, 
                    request.args, 
                    request.nonce,
                    event_tx
                )?;
            }
            
            "DEEP_LINK" => {
                Self::handle_deep_link(request.args, event_tx)?;
            }
            
            _ => {
                eprintln!("Unknown command: {}", request.cmd);
            }
        }
        
        Ok(())
    }
}
```

### 4. Specific Command Handlers
```rust
impl RpcServer {
    fn handle_connections_callback(
        socket_info: SocketInfo,
        nonce: Option<String>,
    ) -> Result<(), RpcError> {
        let response = RpcMessage {
            cmd: "CONNECTIONS_CALLBACK".to_string(),
            data: Some(serde_json::json!({ "code": 1000 })),
            evt: Some("ERROR".to_string()),
            nonce,
        };
        
        socket_info.sender.send(response)
            .map_err(|_| RpcError::SendError)?;
        
        Ok(())
    }
    
    fn handle_set_activity(
        socket_id: u32,
        mut socket_info: SocketInfo,
        args: Option<Value>,
        nonce: Option<String>,
        active_sockets: &Arc<Mutex<HashMap<u32, SocketInfo>>>,
        event_tx: &mpsc::UnboundedSender<RpcEvent>,
    ) -> Result<(), RpcError> {
        let args: SetActivityArgs = args
            .map(serde_json::from_value)
            .transpose()
            .map_err(RpcError::DeserializationError)?
            .unwrap_or_default();
        
        // Update last PID
        if let Some(pid) = args.pid {
            socket_info.last_pid = Some(pid);
            let mut sockets = active_sockets.lock().unwrap();
            sockets.insert(socket_id, socket_info.clone());
        }
        
        match args.activity {
            None => {
                // Activity clear
                let response = RpcMessage {
                    cmd: "SET_ACTIVITY".to_string(),
                    data: None,
                    evt: None,
                    nonce,
                };
                
                socket_info.sender.send(response)
                    .map_err(|_| RpcError::SendError)?;
                
                let _ = event_tx.send(RpcEvent::Activity {
                    activity: None,
                    pid: args.pid,
                    socket_id: socket_id.to_string(),
                });
            }
            
            Some(activity) => {
                // Process activity
                let processed = Self::process_activity(activity, &socket_info.client_id)?;
                
                // Send response to client
                let response_data = serde_json::json!({
                    "name": "",
                    "application_id": socket_info.client_id,
                    "type": 0
                });
                
                let response = RpcMessage {
                    cmd: "SET_ACTIVITY".to_string(),
                    data: Some(response_data),
                    evt: None,
                    nonce,
                };
                
                socket_info.sender.send(response)
                    .map_err(|_| RpcError::SendError)?;
                
                // Emit activity event
                let _ = event_tx.send(RpcEvent::Activity {
                    activity: Some(processed),
                    pid: args.pid,
                    socket_id: socket_id.to_string(),
                });
            }
        }
        
        Ok(())
    }
    
    fn process_activity(
        activity: Activity,
        client_id: &str,
    ) -> Result<ProcessedActivity, RpcError> {
        let mut metadata = ActivityMetadata {
            button_urls: None,
        };
        
        let mut button_labels = None;
        
        // Process buttons
        if let Some(ref buttons) = activity.buttons {
            metadata.button_urls = Some(buttons.iter().map(|b| b.url.clone()).collect());
            button_labels = Some(buttons.iter().map(|b| b.label.clone()).collect());
        }
        
        // Process timestamps (convert seconds to milliseconds if needed)
        let timestamps = activity.timestamps.map(|mut ts| {
            if let Some(start) = ts.start {
                if Self::needs_ms_conversion(start) {
                    ts.start = Some(start * 1000);
                }
            }
            if let Some(end) = ts.end {
                if Self::needs_ms_conversion(end) {
                    ts.end = Some(end * 1000);
                }
            }
            ts
        });
        
        Ok(ProcessedActivity {
            application_id: client_id.to_string(),
            name: activity.name.unwrap_or_default(),
            details: activity.details,
            state: activity.state,
            timestamps,
            assets: activity.assets,
            party: activity.party,
            secrets: activity.secrets,
            metadata,
            flags: if activity.instance.unwrap_or(false) { 1 } else { 0 },
            buttons: button_labels,
            activity_type: 0,
        })
    }
    
    fn needs_ms_conversion(timestamp: u64) -> bool {
        // Check if timestamp is in seconds (length difference > 2 digits)
        let now_str = chrono::Utc::now().timestamp_millis().to_string();
        let ts_str = timestamp.to_string();
        now_str.len() - ts_str.len() > 2
    }
    
    fn handle_invite_browser(
        socket_info: SocketInfo,
        args: Option<Value>,
        nonce: Option<String>,
        event_tx: &mpsc::UnboundedSender<RpcEvent>,
    ) -> Result<(), RpcError> {
        let args: BrowserArgs = args
            .ok_or(RpcError::MissingArgs)?
            .try_into()
            .map_err(RpcError::DeserializationError)?;
        
        let callback = Self::create_browser_callback(
            socket_info, 
            "INVITE_BROWSER".to_string(), 
            args.code.clone(), 
            nonce,
            true
        );
        
        let _ = event_tx.send(RpcEvent::Invite { 
            code: args.code, 
            callback 
        });
        
        Ok(())
    }
    
    fn handle_guild_template_browser(
        socket_info: SocketInfo,
        args: Option<Value>,
        nonce: Option<String>,
        event_tx: &mpsc::UnboundedSender<RpcEvent>,
    ) -> Result<(), RpcError> {
        let args: BrowserArgs = args
            .ok_or(RpcError::MissingArgs)?
            .try_into()
            .map_err(RpcError::DeserializationError)?;
        
        let callback = Self::create_browser_callback(
            socket_info, 
            "GUILD_TEMPLATE_BROWSER".to_string(), 
            args.code.clone(), 
            nonce,
            false
        );
        
        let _ = event_tx.send(RpcEvent::GuildTemplate { 
            code: args.code, 
            callback 
        });
        
        Ok(())
    }
    
    fn create_browser_callback(
        socket_info: SocketInfo,
        cmd: String,
        code: String,
        nonce: Option<String>,
        is_invite: bool,
    ) -> Arc<dyn Fn(bool) + Send + Sync> {
        Arc::new(move |is_valid| {
            let response = if is_valid {
                RpcMessage {
                    cmd: cmd.clone(),
                    data: Some(serde_json::json!({ "code": code })),
                    evt: None,
                    nonce: nonce.clone(),
                }
            } else {
                let error_code = if is_invite { 4011 } else { 4017 };
                let message = format!(
                    "Invalid {} id: {}", 
                    if is_invite { "invite" } else { "guild template" }, 
                    code
                );
                
                RpcMessage {
                    cmd: cmd.clone(),
                    data: Some(serde_json::json!({
                        "code": error_code,
                        "message": message
                    })),
                    evt: Some("ERROR".to_string()),
                    nonce: nonce.clone(),
                }
            };
            
            let _ = socket_info.sender.send(response);
        })
    }
    
    fn handle_deep_link(
        args: Option<Value>,
        event_tx: &mpsc::UnboundedSender<RpcEvent>,
    ) -> Result<(), RpcError> {
        let args: DeepLinkArgs = args
            .ok_or(RpcError::MissingArgs)?
            .try_into()
            .map_err(RpcError::DeserializationError)?;
        
        let _ = event_tx.send(RpcEvent::DeepLink { 
            params: args.params 
        });
        
        Ok(())
    }
}
```

### 5. Event Loop and Coordination
```rust
impl RpcServer {
    pub async fn run(mut self) -> Result<(), RpcError> {
        println!("RPC Server started");
        
        while let Some(event) = self.event_rx.recv().await {
            if let Err(e) = self.handle_event(event).await {
                eprintln!("Error handling event: {}", e);
            }
        }
        
        Ok(())
    }
    
    async fn handle_event(&mut self, event: RpcEvent) -> Result<(), RpcError> {
        match event {
            RpcEvent::Connection { socket_id, socket_info } => {
                if self.config.debug_mode {
                    println!("New connection: {} ({})", socket_id, socket_info.client_id);
                }
                // Additional connection handling if needed
            }
            
            RpcEvent::Disconnection { socket_id } => {
                if self.config.debug_mode {
                    println!("Connection closed: {}", socket_id);
                }
                // Additional cleanup if needed
            }
            
            RpcEvent::Message { socket_id, request } => {
                if self.config.debug_mode {
                    println!("Message from {}: {:?}", socket_id, request);
                }
                // Message already processed in handler
            }
            
            RpcEvent::Activity { activity, pid, socket_id } => {
                if self.config.debug_mode {
                    println!("Activity update: socket={}, pid={:?}", socket_id, pid);
                }
                // Forward to bridge server or other consumers
                self.forward_activity(activity, pid, socket_id).await?;
            }
            
            RpcEvent::Invite { code, callback } => {
                if self.config.debug_mode {
                    println!("Invite browser request: {}", code);
                }
                // Validate invite and call callback
                let is_valid = self.validate_invite(&code).await?;
                callback(is_valid);
            }
            
            RpcEvent::GuildTemplate { code, callback } => {
                if self.config.debug_mode {
                    println!("Guild template browser request: {}", code);
                }
                // Validate guild template and call callback
                let is_valid = self.validate_guild_template(&code).await?;
                callback(is_valid);
            }
            
            RpcEvent::DeepLink { params } => {
                if self.config.debug_mode {
                    println!("Deep link: {}", params);
                }
                // Handle deep link
                self.handle_deep_link_event(params).await?;
            }
        }
        
        Ok(())
    }
    
    async fn forward_activity(
        &self,
        activity: Option<ProcessedActivity>,
        pid: Option<u32>,
        socket_id: String,
    ) -> Result<(), RpcError> {
        // This would integrate with the bridge server
        // For now, just log the activity
        if self.config.debug_mode {
            println!("Forwarding activity: {:?}", activity);
        }
        Ok(())
    }
    
    async fn validate_invite(&self, code: &str) -> Result<bool, RpcError> {
        // In a real implementation, this would validate against Discord's API
        // For now, assume all invites are valid
        Ok(true)
    }
    
    async fn validate_guild_template(&self, code: &str) -> Result<bool, RpcError> {
        // In a real implementation, this would validate against Discord's API
        // For now, assume all guild templates are valid
        Ok(true)
    }
    
    async fn handle_deep_link_event(&self, params: String) -> Result<(), RpcError> {
        // Handle deep link processing
        if self.config.debug_mode {
            println!("Processing deep link: {}", params);
        }
        Ok(())
    }
}
```

## Error Handling

### Error Types
```rust
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("Socket {0} not found")]
    SocketNotFound(u32),
    
    #[error("Failed to send message")]
    SendError,
    
    #[error("Missing required arguments")]
    MissingArgs,
    
    #[error("Deserialization error: {0}")]
    DeserializationError(#[from] serde_json::Error),
    
    #[error("IPC error: {0}")]
    IpcError(#[from] IpcError),
    
    #[error("WebSocket error: {0}")]
    WebSocketError(#[from] WsError),
    
    #[error("Process error: {0}")]
    ProcessError(#[from] ProcessError),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}
```

## Transport Integration

### Transport Handler Trait
```rust
#[async_trait]
pub trait Transport {
    async fn start(&mut self, handlers: TransportHandlers) -> Result<(), RpcError>;
    async fn stop(&mut self) -> Result<(), RpcError>;
}

#[derive(Clone)]
pub struct TransportHandlers {
    pub on_connection: Arc<dyn Fn(SocketConnection) + Send + Sync>,
    pub on_message: Arc<dyn Fn(u32, RpcRequest) + Send + Sync>,
    pub on_close: Arc<dyn Fn(u32) + Send + Sync>,
}

pub struct SocketConnection {
    pub socket_id: u32,
    pub client_id: String,
    pub transport_type: TransportType,
    pub sender: mpsc::UnboundedSender<RpcMessage>,
}

impl SocketConnection {
    pub fn send(&self, message: RpcMessage) -> Result<(), RpcError> {
        self.sender.send(message)
            .map_err(|_| RpcError::SendError)
    }
}
```

## Testing Strategy

### Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};
    
    #[tokio::test]
    async fn test_server_initialization() {
        let server = RpcServer::new().await.unwrap();
        assert!(server.ipc_server.is_some());
        assert!(server.ws_server.is_some());
    }
    
    #[tokio::test]
    async fn test_set_activity_processing() {
        let server = RpcServer::new().await.unwrap();
        
        let activity = Activity {
            name: Some("Test Game".to_string()),
            details: Some("In a match".to_string()),
            timestamps: Some(Timestamps {
                start: Some(1640000000), // Seconds timestamp
                end: None,
            }),
            buttons: Some(vec![Button {
                label: "Join Game".to_string(),
                url: "https://example.com".to_string(),
            }]),
            instance: Some(true),
            ..Default::default()
        };
        
        let processed = RpcServer::process_activity(activity, "123456789").unwrap();
        
        assert_eq!(processed.application_id, "123456789");
        assert_eq!(processed.name, "Test Game");
        assert_eq!(processed.flags, 1); // Instance flag
        assert!(processed.timestamps.unwrap().start.unwrap() > 1640000000000); // Converted to ms
        assert!(processed.metadata.button_urls.is_some());
    }
    
    #[tokio::test]
    async fn test_timestamp_conversion() {
        // Test seconds to milliseconds conversion
        assert!(RpcServer::needs_ms_conversion(1640000000)); // 2021 timestamp in seconds
        assert!(!RpcServer::needs_ms_conversion(1640000000000)); // 2021 timestamp in milliseconds
    }
}
```

### Integration Tests
```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;
    
    #[tokio::test]
    async fn test_full_activity_flow() {
        let mut server = RpcServer::new().await.unwrap();
        
        // Simulate connection
        let (socket_tx, mut socket_rx) = mpsc::unbounded_channel();
        let connection = SocketConnection {
            socket_id: 1,
            client_id: "123456789".to_string(),
            transport_type: TransportType::Ipc,
            sender: socket_tx,
        };
        
        // Test connection handling
        (server.create_connection_handler())(connection);
        
        // Should receive READY message
        let ready_msg = timeout(Duration::from_secs(1), socket_rx.recv())
            .await
            .unwrap()
            .unwrap();
        
        assert_eq!(ready_msg.cmd, "DISPATCH");
        assert_eq!(ready_msg.evt, Some("READY".to_string()));
    }
}
```

## Performance Considerations

### 1. Memory Management
- Use `Arc<Mutex<>>` sparingly; prefer message passing for coordination
- Implement connection pooling for frequent connections
- Use weak references where appropriate to prevent cycles

### 2. Concurrency
- Each transport runs on its own task
- Event processing is single-threaded but non-blocking
- Use bounded channels to prevent memory exhaustion

### 3. Error Recovery
- Implement exponential backoff for failed connections
- Graceful degradation when transport layers fail
- Comprehensive logging for debugging

## Logging and Debugging

### Logging Implementation
```rust
use tracing::{info, warn, error, debug};

pub fn setup_logging(debug_mode: bool) {
    let level = if debug_mode {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };
    
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .init();
}

// Usage in server
impl RpcServer {
    fn log_activity(&self, activity: &Option<ProcessedActivity>, socket_id: &str) {
        match activity {
            Some(act) => info!(
                socket_id = socket_id,
                app_id = act.application_id,
                name = act.name,
                "Activity set"
            ),
            None => info!(
                socket_id = socket_id,
                "Activity cleared"
            ),
        }
    }
}
```

## Discord Protocol Compliance

### 1. Message Format Compliance
- All messages must follow Discord's RPC message format exactly
- Proper error codes and messages for protocol violations
- Correct event names and data structures

### 2. Authentication Mock
```rust
pub const MOCK_USER_DATA: serde_json::Value = serde_json::json!({
    "id": "1045800378228281345",
    "username": "arrpc",
    "discriminator": "0",
    "global_name": "arRPC",
    "avatar": "cfefa4d9839fb4bdf030f91c2a13e95c",
    "avatar_decoration_data": null,
    "bot": false,
    "flags": 0,
    "premium_type": 0
});

pub const DISCORD_CONFIG: serde_json::Value = serde_json::json!({
    "cdn_host": "cdn.discordapp.com",
    "api_endpoint": "//discord.com/api",
    "environment": "production"
});
```

### 3. Activity Validation
```rust
impl Activity {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if let Some(ref name) = self.name {
            if name.len() > 128 {
                return Err(ValidationError::NameTooLong);
            }
        }
        
        if let Some(ref details) = self.details {
            if details.len() > 128 {
                return Err(ValidationError::DetailsTooLong);
            }
        }
        
        if let Some(ref state) = self.state {
            if state.len() > 128 {
                return Err(ValidationError::StateTooLong);
            }
        }
        
        if let Some(ref buttons) = self.buttons {
            if buttons.len() > 2 {
                return Err(ValidationError::TooManyButtons);
            }
            
            for button in buttons {
                if button.label.len() > 32 {
                    return Err(ValidationError::ButtonLabelTooLong);
                }
                
                if !button.url.starts_with("http") {
                    return Err(ValidationError::InvalidButtonUrl);
                }
            }
        }
        
        Ok(())
    }
}
```

This comprehensive implementation plan provides the foundation for implementing a robust RPC Server in Rust that maintains full compatibility with Discord's Rich Presence Protocol while providing the coordination and event handling necessary for the arRPC system.