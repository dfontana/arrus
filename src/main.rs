mod activity;
mod bridge;
mod config;
mod database;
mod detector;

use bridge::BridgeServer;
use config::load_config;
use detector::ProcessDetector;
use kitchen_sink::logging::{self, set_log_level};
use tracing::{error, info};

/*
DB calls discord to enumerate viable games
-> Detector scans on loop looking for game matches
-> Detector sends matched game activity to Bridge
-> When game no longer found, sends null activity to Bridge

Bridge is a WSS which:
- Tracks active clients
- Stores last message and forwards to each client
- Forwards activity.
*/
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    logging::initalize_logging();

    let config = load_config()?;
    info!("Loaded config: {:#?}", config);
    set_log_level(config.log_level)?;

    let bridge_server = BridgeServer::new(config.bridge.clone())?;
    let sender = bridge_server.get_sender();

    let process_detector =
        ProcessDetector::new_with_manager(sender.clone(), config.database.clone()).await?;

    tokio::select! {
        res = bridge_server.start() => {
          if let Err(why) = res {
            error!("Bridge service failed: {:?}", why);
          }
        }
        _ = process_detector.start() => {
            info!("Process detector exited");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Shutting down");
        }
    }

    Ok(())
}
