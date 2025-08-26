use tracing::Level;

use crate::bridge::BridgeConfig;
use crate::database::{DatabaseConfig, HttpConfig};
use std::env;
use std::time::Duration;

#[derive(Debug)]
pub struct Config {
    pub bridge: BridgeConfig,
    pub database: DatabaseConfig,
    pub log_level: Level,
}

pub fn load_config() -> Config {
    let mut config = Config {
        bridge: BridgeConfig {
            port: 1337,
            bind_address: "127.0.0.1".to_string(),
        },
        database: DatabaseConfig {
            http: HttpConfig {
                base_url: "https://discord.com/api/v9".to_string(),
                endpoint: "/applications/detectable".to_string(),
                user_agent: "arrus/1.0".to_string(),
                timeout: Duration::from_secs(30),
                connect_timeout: Duration::from_secs(10),
                max_retries: 3,
            },
            update_interval: Duration::from_secs(15 * 60),
        },
        log_level: Level::INFO,
    };

    // DB HTTP configuration
    if let Ok(base_url) = env::var("ARRUS_DB_BASE_URL") {
        config.database.http.base_url = base_url;
    }

    if let Ok(endpoint) = env::var("ARRUS_DB_ENDPOINT") {
        config.database.http.endpoint = endpoint;
    }

    if let Ok(user_agent) = env::var("ARRUS_DB_USER_AGENT") {
        config.database.http.user_agent = user_agent;
    }

    if let Ok(timeout_str) = env::var("ARRUS_DB_TIMEOUT") {
        if let Ok(timeout_secs) = timeout_str.parse::<u64>() {
            config.database.http.timeout = Duration::from_secs(timeout_secs);
        }
    }

    if let Ok(retries_str) = env::var("ARRUS_DB_MAX_RETRIES") {
        if let Ok(retries) = retries_str.parse::<u32>() {
            config.database.http.max_retries = retries;
        }
    }

    // DB Scheduler configuration
    if let Ok(interval_str) = env::var("ARRUS_DB_UPDATE_INTERVAL") {
        if let Ok(interval_hours) = interval_str.parse::<u64>() {
            config.database.update_interval = Duration::from_secs(interval_hours * 3600);
        }
    }

    // Bridge configuration
    if let Ok(port_str) = std::env::var("ARRPC_BRIDGE_PORT") {
        if let Ok(port) = port_str.parse::<u16>() {
            config.bridge.port = port;
        }
    }

    // Logger
    if let Ok(log_level_str) = env::var("ARRUS_LOG_LEVEL") {
        if let Ok(log_level) = log_level_str.parse::<Level>() {
            config.log_level = log_level;
        }
    }

    config
}
