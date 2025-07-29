use crate::activity::ActivityMessage;
use crate::error::BridgeError;
use crate::logger::Logger;
use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc};

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

impl BridgeConfig {
    pub fn from_env() -> Result<Self, BridgeError> {
        let mut config = Self::default();

        if let Ok(port_str) = std::env::var("ARRPC_BRIDGE_PORT") {
            config.port = port_str
                .parse()
                .map_err(|_| BridgeError::InvalidPort(port_str))?;
        }

        Ok(config)
    }
}

#[derive(Clone)]
pub struct AppState {
    last_messages: Arc<RwLock<HashMap<String, ActivityMessage>>>,
    broadcast_tx: broadcast::Sender<ActivityMessage>,
    logger: Logger,
}

pub struct BridgeServer {
    state: AppState,
    config: BridgeConfig,
    activity_tx: mpsc::UnboundedSender<ActivityMessage>,
}

impl BridgeServer {
    pub fn new() -> Result<Self, BridgeError> {
        let config = BridgeConfig::from_env()?;
        let logger = Logger::new("bridge");
        let (broadcast_tx, _) = broadcast::channel(100);
        let (activity_tx, mut activity_rx) = mpsc::unbounded_channel::<ActivityMessage>();

        let state = AppState {
            last_messages: Arc::new(RwLock::new(HashMap::new())),
            broadcast_tx: broadcast_tx.clone(),
            logger: logger.clone(),
        };

        let state_clone = state.clone();
        tokio::spawn(async move {
            while let Some(message) = activity_rx.recv().await {
                {
                    let mut cache = state_clone.last_messages.write().await;
                    cache.insert(message.socket_id.clone(), message.clone());
                }

                if state_clone.broadcast_tx.send(message).is_err() {
                    state_clone.logger.error("Failed to broadcast message");
                }
            }
        });

        Ok(Self {
            state,
            config,
            activity_tx,
        })
    }

    pub async fn start(&self) -> Result<(), BridgeError> {
        let app = Router::new()
            .route("/", get(websocket_handler))
            .with_state(self.state.clone());

        let addr = format!("{}:{}", self.config.bind_address, self.config.port);
        self.state
            .logger
            .info(&format!("listening on {}", self.config.port));

        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| BridgeError::BindFailed(addr.clone(), e))?;

        axum::serve(listener, app)
            .await
            .map_err(BridgeError::ClientError)?;

        Ok(())
    }

    pub fn get_sender(&self) -> mpsc::UnboundedSender<ActivityMessage> {
        self.activity_tx.clone()
    }
}

async fn websocket_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    state.logger.info("web connected");

    let (mut sender, mut receiver) = socket.split();
    let mut broadcast_rx = state.broadcast_tx.subscribe();

    {
        let messages = state.last_messages.read().await;
        for (_, message) in messages.iter() {
            if message.activity.is_some() {
                if let Ok(json) = serde_json::to_string(message) {
                    if sender.send(Message::Text(json)).await.is_err() {
                        state.logger.error("Failed to send catch-up message");
                        return;
                    }
                }
            }
        }
    }

    let send_task = tokio::spawn(async move {
        while let Ok(message) = broadcast_rx.recv().await {
            if let Ok(json) = serde_json::to_string(&message) {
                if sender.send(Message::Text(json)).await.is_err() {
                    break;
                }
            }
        }
    });

    let recv_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Close(_)) => break,
                Ok(Message::Ping(_data)) => {
                    // Axum handles pong automatically
                }
                Err(_) => break,
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = send_task => {},
        _ = recv_task => {},
    }

    state.logger.info("web disconnected");
}
