use crate::activity::ActivityMessage;
use anyhow::anyhow;
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
use tracing::{error, info, instrument};

#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub port: u16,
    pub bind_address: String,
}

#[derive(Clone)]
pub struct AppState {
    last_messages: Arc<RwLock<HashMap<String, ActivityMessage>>>,
    broadcast_tx: broadcast::Sender<ActivityMessage>,
}

pub struct BridgeServer {
    state: AppState,
    config: BridgeConfig,
    activity_tx: mpsc::UnboundedSender<ActivityMessage>,
}

impl BridgeServer {
    pub fn new(config: BridgeConfig) -> Result<Self, anyhow::Error> {
        let (broadcast_tx, _) = broadcast::channel(100);
        let (activity_tx, mut activity_rx) = mpsc::unbounded_channel::<ActivityMessage>();

        let state = AppState {
            last_messages: Arc::new(RwLock::new(HashMap::new())),
            broadcast_tx: broadcast_tx.clone(),
        };

        let state_clone = state.clone();
        tokio::spawn(async move {
            while let Some(message) = activity_rx.recv().await {
                {
                    let mut cache = state_clone.last_messages.write().await;
                    cache.insert(message.socket_id.clone(), message.clone());
                }

                if let Err(e) = state_clone.broadcast_tx.send(message) {
                    error!("Failed to broadcast message: {e}");
                }
            }
        });

        Ok(Self {
            state,
            config,
            activity_tx,
        })
    }

    pub async fn start(&self) -> Result<(), anyhow::Error> {
        let app = Router::new()
            .route("/", get(websocket_handler))
            .with_state(self.state.clone());

        let addr = format!("{}:{}", self.config.bind_address, 1337);
        info!("listening on {addr}");

        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| anyhow!("Bind failed to {} {:?}", addr.clone(), e))?;

        axum::serve(listener, app).await?;
        Ok(())
    }

    pub fn get_sender(&self) -> mpsc::UnboundedSender<ActivityMessage> {
        self.activity_tx.clone()
    }
}

async fn websocket_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

#[instrument(skip(socket, state))]
async fn handle_socket(socket: WebSocket, state: AppState) {
    info!("Bridge client connected");

    let (mut sender, mut receiver) = socket.split();
    let mut broadcast_rx = state.broadcast_tx.subscribe();

    // TODO: BUG -> If there's not active connections when a game is detected then nothing is broadcast
    //      so that does make the following required, but ideally that's not the case
    // TODO: This shouldn't be needed b/c the broadcast channel holds state
    // {
    //     let messages = state.last_messages.read().await;
    //     for (_, message) in messages.iter() {
    //         if message.activity.is_some() {
    //             if let Ok(json) = serde_json::to_string(message) {
    //                 if sender.send(Message::Text(json)).await.is_err() {
    //                     error!("Failed to send catch-up message");
    //                     return;
    //                 }
    //             }
    //         }
    //     }
    // }

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

    info!("web disconnected");
}
