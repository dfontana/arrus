mod handlers;
mod types;

use handlers::*;
use std::sync::Arc;
use tracing::{error, info};
pub use types::*;

/// Main RPC server structure
pub struct RpcServer {
    socket_counter: SocketCounter,
    active_sockets: ActiveSockets,
    event_tx: EventSender,
    event_rx: EventReceiver,
}

impl RpcServer {
    /// Create a new RPC server instance
    pub fn new() -> Self {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        RpcServer {
            socket_counter: Arc::new(std::sync::Mutex::new(0)),
            active_sockets: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            event_tx,
            event_rx,
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
                data: Some(serde_json::json!({
                    "v": 1,
                    "config": serde_json::json!({
                        "cdn_host": "cdn.discordapp.com",
                        "api_endpoint": "//discord.com/api",
                        "environment": "production"
                    }),
                    "user": serde_json::json!({
                        "id": "1045800378228281345",
                        "username": "arrpc",
                        "discriminator": "0",
                        "global_name": "arRPC",
                        "avatar": "cfefa4d9839fb4bdf030f91c2a13e95c",
                        "avatar_decoration_data": null,
                        "bot": false,
                        "flags": 0,
                        "premium_type": 0
                    })
                })),
                evt: Some(RpcEventType::Ready),
                nonce: None,
            };

            // Send ready message
            if let Err(e) = socket.send(ready_message) {
                error!("Failed to send READY message: {e}");
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
                error!("Error processing command: {e}");
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
    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        info!("RPC Server started");
        while let Some(event) = self.event_rx.recv().await {
            if let Err(e) = self.handle_event(event).await {
                error!("Error handling event: {e}");
            }
        }
        Ok(())
    }

    /// Handle events from the event loop
    async fn handle_event(&mut self, event: RpcEvent) -> Result<(), anyhow::Error> {
        match event {
            RpcEvent::Connection {
                socket_id,
                socket_info,
            } => {
                info!("New connection: {} ({})", socket_id, socket_info.client_id);
            }
            RpcEvent::Disconnection { socket_id } => {
                info!("Connection closed: {socket_id}");
            }
            RpcEvent::Message { socket_id, request } => {
                info!("Message from {socket_id}: {request:?}");
            }
            RpcEvent::Activity {
                activity,
                pid,
                socket_id,
            } => {
                info!("Activity update: socket={socket_id}, pid={pid:?}");
                self.forward_activity(activity, pid, socket_id).await?;
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
    ) -> Result<(), anyhow::Error> {
        info!("Forwarding activity: {activity:?}");
        // TODO: Integrate with bridge server
        Ok(())
    }
}
