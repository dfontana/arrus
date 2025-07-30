mod activity;
mod bridge;
mod error;
mod logger;
mod server;
mod transports;

use activity::{ActivityData, ActivityMessage, ActivityTimestamps};
use bridge::BridgeServer;
use logger::Logger;
use server::RpcServer;
use transports::WebSocketTransport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let logger = Logger::new("main");
    logger.info("Starting arRPC server suite");

    // Initialize bridge server
    let bridge_server = BridgeServer::new()?;
    let sender = bridge_server.get_sender();

    // Initialize RPC server
    let rpc_server = RpcServer::new();
    let transport_handlers = rpc_server.get_transport_handlers();

    // Initialize WebSocket transport
    let mut websocket_transport = WebSocketTransport::new();

    // Start bridge server
    let bridge_handle = tokio::spawn(async move {
        if let Err(e) = bridge_server.start().await {
            eprintln!("Bridge server error: {}", e);
        }
    });

    // Start RPC server
    let rpc_handle = tokio::spawn(async move {
        if let Err(e) = rpc_server.run().await {
            eprintln!("RPC server error: {}", e);
        }
    });

    // Start WebSocket transport
    let websocket_handle = tokio::spawn(async move {
        if let Err(e) = websocket_transport.start(transport_handlers).await {
            eprintln!("WebSocket transport error: {}", e);
        }
    });

    // Test activity after brief delay
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        let test_activity = ActivityMessage {
            socket_id: "test-socket".to_string(),
            activity: Some(ActivityData {
                application_id: "123456789".to_string(),
                name: "Test Game".to_string(),
                details: Some("In a test level".to_string()),
                state: Some("Playing".to_string()),
                timestamps: Some(ActivityTimestamps {
                    start: Some(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    ),
                    end: None,
                }),
                assets: None,
                party: None,
                secrets: None,
                instance: None,
                flags: None,
                buttons: None,
                metadata: None,
                activity_type: 0,
            }),
            pid: Some(12345),
        };

        if let Err(e) = sender.send(test_activity) {
            eprintln!("Failed to send test activity: {}", e);
        }
    });

    tokio::select! {
        _ = bridge_handle => {
            logger.error("Bridge server exited");
        }
        _ = rpc_handle => {
            logger.error("RPC server exited");
        }
        _ = websocket_handle => {
            logger.error("WebSocket transport exited");
        }
        _ = tokio::signal::ctrl_c() => {
            logger.info("Shutting down");
        }
    }

    Ok(())
}
