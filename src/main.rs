mod activity;
mod bridge;
mod config;
mod connection_manager;
mod database;
mod detector;
mod server;
mod transports;

use bridge::BridgeServer;
use config::load_database_config;
use detector::ProcessDetector;
use kitchen_sink::logging;
use tracing::{error, info};
use transports::WebSocketTransport;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    logging::initalize_logging();

    /*
    DB calls discord to enumerate viable games
    -> Detector scans on loop looking for game matches
    -> Detector sends matched game activity to Bridge

    Bridge
    -> Broadcasts activity to subscribers on it's own websocket connections
      -> Both pre-existing when new connection is made
      -> And new as activity comes in
    -> Reference impl only has this operate for webapp; should sniff vesktop to see what it uses
       but any RPC activity events are forwarded to bridge (as described above). So likely not
       a needed component
       -> This also means IPC server may not be needed which would be nice b/c websocket looks
          simpler. Websocket and IPC both exist together at the same time though. If bridge is
          for webapp, why is Websocket+IPC both needed? Diff client types? What does vesktop use?
       -> IPC is the formal protocol for apps to speak to the discord desktop app (like spotify for ex)
       -> Websocket

    // TODO: What do these things do tho?
    RPC -> (Not fully impl'd) Sends activity to Bridge, but not clear from what?
    Transports -> Websocket?? IPC?? Who are these for?
    */

    // Initialize bridge server
    let bridge_server = BridgeServer::new()?;
    let sender = bridge_server.get_sender();

    // Initialize RPC server
    // TODO -> RPC server should be shared logic between WS/IPC which they both
    // call after their own logic for OnConn/OnMsg/OnClose. Start both of them
    // here or a factory returning both handles
    let ipc_rpc = RpcServer::new();
    let mut ws_rpc = WebSocketTransport::new();

    // Initialize process detector with database manager
    let db_config = load_database_config();
    let process_detector =
        ProcessDetector::new_with_manager(sender.clone(), Some(db_config)).await?;

    // Start bridge server
    let bridge_handle = tokio::spawn(async move {
        if let Err(e) = bridge_server.start().await {
            eprintln!("Bridge server error: {e}");
        }
    });

    // Start RPC servers
    let rpc_handle = tokio::spawn(async move {
        if let Err(e) = ipc_rpc.run().await {
            eprintln!("RPC server error: {e}");
        }
    });
    let websocket_handle = tokio::spawn(async move {
        if let Err(e) = ws_rpc.start().await {
            eprintln!("WebSocket transport error: {e}");
        }
    });

    // Start process detector
    let process_handle = process_detector.start();

    tokio::select! {
        _ = bridge_handle => {
            error!("Bridge server exited");
        }
        _ = rpc_handle => {
            error!("RPC server exited");
        }
        _ = websocket_handle => {
            error!("WebSocket transport exited");
        }
        _ = process_handle => {
            error!("Process detector exited");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Shutting down");
        }
    }

    Ok(())
}
