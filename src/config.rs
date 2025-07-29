use crate::db::DatabaseConfig;
use std::env;
use std::path::PathBuf;
use std::time::Duration;

pub fn load_database_config() -> DatabaseConfig {
    let mut config = DatabaseConfig::default();

    // HTTP configuration
    if let Ok(base_url) = env::var("ARRPC_DB_BASE_URL") {
        config.http.base_url = base_url;
    }

    if let Ok(endpoint) = env::var("ARRPC_DB_ENDPOINT") {
        config.http.endpoint = endpoint;
    }

    if let Ok(user_agent) = env::var("ARRPC_DB_USER_AGENT") {
        config.http.user_agent = user_agent;
    }

    if let Ok(timeout_str) = env::var("ARRPC_DB_TIMEOUT") {
        if let Ok(timeout_secs) = timeout_str.parse::<u64>() {
            config.http.timeout = Duration::from_secs(timeout_secs);
        }
    }

    if let Ok(retries_str) = env::var("ARRPC_DB_MAX_RETRIES") {
        if let Ok(retries) = retries_str.parse::<u32>() {
            config.http.max_retries = retries;
        }
    }

    // File paths
    if let Ok(db_file) = env::var("ARRPC_DB_FILE") {
        config.file_paths.database_file = PathBuf::from(db_file);
    }

    if let Ok(backup_dir) = env::var("ARRPC_DB_BACKUP_DIR") {
        config.file_paths.backup_directory = PathBuf::from(backup_dir);
    }

    if let Ok(temp_dir) = env::var("ARRPC_DB_TEMP_DIR") {
        config.file_paths.temp_directory = PathBuf::from(temp_dir);
    }

    // Scheduler configuration
    if let Ok(auto_updates_str) = env::var("ARRPC_DB_AUTO_UPDATES") {
        config.scheduler.enable_auto_updates = auto_updates_str.to_lowercase() == "true";
    }

    if let Ok(interval_str) = env::var("ARRPC_DB_UPDATE_INTERVAL") {
        if let Ok(interval_hours) = interval_str.parse::<u64>() {
            config.scheduler.update_interval = Duration::from_secs(interval_hours * 3600);
        }
    }

    if let Ok(startup_delay_str) = env::var("ARRPC_DB_STARTUP_DELAY") {
        if let Ok(delay_minutes) = startup_delay_str.parse::<u64>() {
            config.scheduler.startup_delay = Duration::from_secs(delay_minutes * 60);
        }
    }

    // Validation configuration
    if let Ok(strict_mode_str) = env::var("ARRPC_DB_STRICT_MODE") {
        config.validation.strict_mode = strict_mode_str.to_lowercase() == "true";
    }

    if let Ok(max_size_str) = env::var("ARRPC_DB_MAX_SIZE") {
        if let Ok(max_size) = max_size_str.parse::<u64>() {
            config.validation.max_database_size = max_size;
        }
    }

    if let Ok(min_games_str) = env::var("ARRPC_DB_MIN_GAMES") {
        if let Ok(min_games) = min_games_str.parse::<usize>() {
            config.validation.min_game_count = min_games;
        }
    }

    // Backup configuration
    if let Ok(max_backups_str) = env::var("ARRPC_DB_MAX_BACKUPS") {
        if let Ok(max_backups) = max_backups_str.parse::<usize>() {
            config.backup.max_backups = max_backups;
        }
    }

    if let Ok(compress_str) = env::var("ARRPC_DB_COMPRESS_BACKUPS") {
        config.backup.compress_backups = compress_str.to_lowercase() == "true";
    }

    if let Ok(backup_on_update_str) = env::var("ARRPC_DB_BACKUP_ON_UPDATE") {
        config.backup.backup_on_update = backup_on_update_str.to_lowercase() == "true";
    }

    config
}
