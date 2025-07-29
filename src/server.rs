use std::sync::Arc;

use crate::error::ArrusError;

mod handlers;
mod protocol;
mod types;

use handlers::*;
use protocol::*;
pub use types::*;

/// Main RPC server structure
pub struct RpcServer {
    socket_counter: SocketCounter,
    active_sockets: ActiveSockets,
    event_tx: EventSender,
    event_rx: EventReceiver,
    config: RpcConfig,
}

impl RpcServer {
    /// Create a new RPC server instance
    pub fn new() -> Self {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let config = RpcConfig::default();

        RpcServer {
            socket_counter: Arc::new(std::sync::Mutex::new(0)),
            active_sockets: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            event_tx,
            event_rx,
            config,
        }
    }

    /// Get transport handlers for different transport types
    pub fn get_transport_handlers(&self) -> TransportHandlers {
        TransportHandlers {
            on_connection: self.create_connection_handler(),
            on_message: self.create_message_handler(),
            on_close: self.create_close_handler(),
        }
    }

    /// Create connection handler
    fn create_connection_handler(&self) -> Arc<dyn Fn(SocketConnection) + Send + Sync> {
        let event_tx = self.event_tx.clone();
        let socket_counter = self.socket_counter.clone();
        let active_sockets = self.active_sockets.clone();

        Arc::new(move |socket: SocketConnection| {
            // Generate unique socket ID
            let socket_id = {
                let mut counter = socket_counter.lock().unwrap();
                *counter += 1;
                *counter
            };

            // Send Discord READY event
            let ready_message = RpcMessage {
                cmd: RpcCommand::Dispatch,
                data: Some(create_ready_payload()),
                evt: Some(RpcEventType::Ready),
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
            let _ = event_tx.send(RpcEvent::Connection {
                socket_id,
                socket_info,
            });
        })
    }

    /// Create message handler
    fn create_message_handler(&self) -> Arc<dyn Fn(u32, RpcRequest) + Send + Sync> {
        let event_tx = self.event_tx.clone();
        let active_sockets = self.active_sockets.clone();

        Arc::new(move |socket_id: u32, request: RpcRequest| {
            // Emit message event first
            let _ = event_tx.send(RpcEvent::Message {
                socket_id,
                request: request.clone(),
            });

            // Process specific commands
            if let Err(e) = process_command(socket_id, request, &active_sockets, &event_tx) {
                eprintln!("Error processing command: {}", e);
            }
        })
    }

    /// Create close handler
    fn create_close_handler(&self) -> Arc<dyn Fn(u32) + Send + Sync> {
        let event_tx = self.event_tx.clone();
        let active_sockets = self.active_sockets.clone();

        Arc::new(move |socket_id: u32| {
            // Get socket info before removal
            let socket_info = {
                let mut sockets = active_sockets.lock().unwrap();
                sockets.remove(&socket_id)
            };

            if let Some(info) = socket_info {
                // Emit activity clear event
                let _ = event_tx.send(RpcEvent::Activity {
                    activity: Box::new(None),
                    pid: info.last_pid,
                    socket_id: socket_id.to_string(),
                });

                // Emit disconnection event
                let _ = event_tx.send(RpcEvent::Disconnection { socket_id });
            }
        })
    }

    /// Run the RPC server event loop
    pub async fn run(mut self) -> Result<(), ArrusError> {
        println!("RPC Server started");

        while let Some(event) = self.event_rx.recv().await {
            if let Err(e) = self.handle_event(event).await {
                eprintln!("Error handling event: {}", e);
            }
        }

        Ok(())
    }

    /// Handle events from the event loop
    async fn handle_event(&mut self, event: RpcEvent) -> Result<(), ArrusError> {
        match event {
            RpcEvent::Connection {
                socket_id,
                socket_info,
            } => {
                if self.config.debug_mode {
                    println!("New connection: {} ({})", socket_id, socket_info.client_id);
                }
            }

            RpcEvent::Disconnection { socket_id } => {
                if self.config.debug_mode {
                    println!("Connection closed: {}", socket_id);
                }
            }

            RpcEvent::Message { socket_id, request } => {
                if self.config.debug_mode {
                    println!("Message from {}: {:?}", socket_id, request);
                }
            }

            RpcEvent::Activity {
                activity,
                pid,
                socket_id,
            } => {
                if self.config.debug_mode {
                    println!("Activity update: socket={}, pid={:?}", socket_id, pid);
                }
                self.forward_activity(activity, pid, socket_id).await?;
            }

            RpcEvent::Invite { code, callback } => {
                if self.config.debug_mode {
                    println!("Invite browser request: {}", code);
                }
                let is_valid = self.validate_invite(&code).await?;
                callback(is_valid);
            }

            RpcEvent::GuildTemplate { code, callback } => {
                if self.config.debug_mode {
                    println!("Guild template browser request: {}", code);
                }
                let is_valid = self.validate_guild_template(&code).await?;
                callback(is_valid);
            }

            RpcEvent::DeepLink { params } => {
                if self.config.debug_mode {
                    println!("Deep link: {}", params);
                }
                self.handle_deep_link_event(params).await?;
            }
        }

        Ok(())
    }

    /// Forward activity to bridge server
    async fn forward_activity(
        &self,
        activity: Box<Option<ProcessedActivity>>,
        _pid: Option<u32>,
        _socket_id: String,
    ) -> Result<(), ArrusError> {
        if self.config.debug_mode {
            println!("Forwarding activity: {:?}", activity);
        }
        // TODO: Integrate with bridge server
        Ok(())
    }

    /// Validate invite code (stub implementation)
    async fn validate_invite(&self, _code: &str) -> Result<bool, ArrusError> {
        // TODO: Implement actual validation against Discord API
        Ok(true)
    }

    /// Validate guild template code (stub implementation)
    async fn validate_guild_template(&self, _code: &str) -> Result<bool, ArrusError> {
        // TODO: Implement actual validation against Discord API
        Ok(true)
    }

    /// Handle deep link event (stub implementation)
    async fn handle_deep_link_event(&self, params: String) -> Result<(), ArrusError> {
        if self.config.debug_mode {
            println!("Processing deep link: {}", params);
        }
        // TODO: Implement deep link handling
        Ok(())
    }
}
