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
use tokio::sync::{RwLock, broadcast};
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
    activity_tx: broadcast::Sender<ActivityMessage>,
}

impl BridgeServer {
    pub fn new(config: BridgeConfig) -> Result<Self, anyhow::Error> {
        let (activity_tx, _) = broadcast::channel(100);

        let state = AppState {
            last_messages: Arc::new(RwLock::new(HashMap::new())),
            broadcast_tx: activity_tx.clone(),
        };

        // Cache subscriber task
        let state_clone = state.clone();
        let mut cache_rx = activity_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(message) = cache_rx.recv().await {
                // Socket is just gameId. This technically grows unbounded but system would need to be up
                // a long time and a lot of unique games running, so probably fine
                let mut cache = state_clone.last_messages.write().await;
                cache.insert(message.socket_id.clone(), message.clone());
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

    pub fn get_sender(&self) -> broadcast::Sender<ActivityMessage> {
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

    {
        // Catch up to the activity feed, eg if a game was detected but there was no discord client this
        // will feed it in.
        let messages = state.last_messages.read().await;
        for (_, message) in messages.iter() {
            if message.has_activity() {
                if let Ok(json) = serde_json::to_string(message) {
                    if sender.send(Message::Text(json)).await.is_err() {
                        error!("Failed to send catch-up message");
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

    info!("web disconnected");
}
