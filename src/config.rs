use crate::database::DatabaseConfig;
use std::env;
use std::time::Duration;

pub fn load_database_config() -> DatabaseConfig {
    let mut config = DatabaseConfig::default();

    // HTTP configuration
    if let Ok(base_url) = env::var("ARRUS_DB_BASE_URL") {
        config.http.base_url = base_url;
    }

    if let Ok(endpoint) = env::var("ARRUS_DB_ENDPOINT") {
        config.http.endpoint = endpoint;
    }

    if let Ok(user_agent) = env::var("ARRUS_DB_USER_AGENT") {
        config.http.user_agent = user_agent;
    }

    if let Ok(timeout_str) = env::var("ARRUS_DB_TIMEOUT") {
        if let Ok(timeout_secs) = timeout_str.parse::<u64>() {
            config.http.timeout = Duration::from_secs(timeout_secs);
        }
    }

    if let Ok(retries_str) = env::var("ARRUS_DB_MAX_RETRIES") {
        if let Ok(retries) = retries_str.parse::<u32>() {
            config.http.max_retries = retries;
        }
    }

    // Scheduler configuration
    if let Ok(interval_str) = env::var("ARRUS_DB_UPDATE_INTERVAL") {
        if let Ok(interval_hours) = interval_str.parse::<u64>() {
            config.update_interval = Duration::from_secs(interval_hours * 3600);
        }
    }

    config
}
