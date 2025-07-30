mod activity;
mod bridge;
mod config;
mod db;
mod error;
mod logger;
mod process;
mod server;
mod transports;

use bridge::BridgeServer;
use config::load_database_config;
use logger::Logger;
use process::ProcessDetector;
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

    // Initialize process detector with database manager
    let db_config = load_database_config();
    let process_detector =
        ProcessDetector::new_with_manager(sender.clone(), Some(db_config)).await?;

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

    // Start process detector
    let process_handle = process_detector.start();

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
        _ = process_handle => {
            logger.error("Process detector exited");
        }
        _ = tokio::signal::ctrl_c() => {
            logger.info("Shutting down");
        }
    }

    Ok(())
}
