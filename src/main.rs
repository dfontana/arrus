mod activity;
mod bridge;
mod error;
mod logger;

use activity::{ActivityData, ActivityMessage, ActivityTimestamps};
use bridge::BridgeServer;
use logger::Logger;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let logger = Logger::new("main");
    logger.info("Starting arRPC bridge server");

    let bridge_server = BridgeServer::new()?;
    let sender = bridge_server.get_sender();

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
        result = bridge_server.start() => {
            if let Err(e) = result {
                logger.error(&format!("Failed to start bridge server: {}", e));
                return Err(e.into());
            }
        }
        _ = tokio::signal::ctrl_c() => {
            logger.info("Shutting down");
        }
    }

    Ok(())
}
