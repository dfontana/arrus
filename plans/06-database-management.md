# Database Management Implementation Plan

## Overview

The Database Management component handles downloading, updating, and managing Discord's detectable games database. This component is responsible for maintaining an up-to-date local copy of the games database used by the Process Detection component for identifying running applications.

## Core Responsibilities

1. **Database Download**: Fetch the latest games database from Discord's API
2. **Validation**: Ensure downloaded data integrity and schema compliance
3. **Atomic Updates**: Safely replace the existing database without data corruption
4. **Backup Management**: Maintain backup copies for rollback scenarios
5. **Update Scheduling**: Handle automatic and manual update triggers
6. **Error Recovery**: Robust handling of network failures and corrupted data
7. **Integration**: Seamless coordination with Process Detection component

## Data Structure Analysis

Based on the existing `detectable.json` (10,033 games, 3.7MB), each game entry contains:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameEntry {
    pub id: String,                    // Discord application ID
    pub name: String,                  // Display name
    pub aliases: Vec<String>,          // Alternative names
    pub executables: Vec<Executable>,  // Process executables
    pub hook: bool,                    // Hook capability
    pub icon_hash: Option<String>,     // Icon identifier
    pub overlay: bool,                 // Overlay support
    pub overlay_compatibility_hook: bool,
    pub overlay_methods: Option<u32>,
    pub overlay_warn: bool,
    pub themes: Vec<String>,           // Game categories
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Executable {
    pub name: String,                  // Executable path/name
    pub os: String,                    // Target OS: "linux", "win32", "darwin"
    pub is_launcher: bool,             // Is game launcher vs game itself
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<String>,     // Required command line args
}
```

## Core Components

### 1. Database Manager (`database_manager.rs`)

Main orchestrator for all database operations:

```rust
pub struct DatabaseManager {
    config: DatabaseConfig,
    http_client: HttpClient,
    file_manager: FileManager,
    validator: DatabaseValidator,
    scheduler: UpdateScheduler,
    backup_manager: BackupManager,
}

impl DatabaseManager {
    pub async fn new(config: DatabaseConfig) -> Result<Self>;
    pub async fn initialize(&mut self) -> Result<()>;
    pub async fn update_database(&mut self) -> Result<UpdateResult>;
    pub async fn force_update(&mut self) -> Result<UpdateResult>;
    pub async fn rollback_to_backup(&mut self, backup_id: &str) -> Result<()>;
    pub async fn validate_current_database(&self) -> Result<ValidationResult>;
    pub fn get_database_info(&self) -> DatabaseInfo;
    pub async fn shutdown(&mut self) -> Result<()>;
}
```

### 2. HTTP Client (`http_client.rs`)

Handles API communication with Discord's endpoint:

```rust
pub struct HttpClient {
    client: reqwest::Client,
    config: HttpConfig,
    retry_policy: RetryPolicy,
}

impl HttpClient {
    pub fn new(config: HttpConfig) -> Self;
    pub async fn download_database(&self) -> Result<DatabaseResponse>;
    pub async fn download_with_etag(&self, etag: Option<&str>) -> Result<DatabaseResponse>;
    pub async fn check_database_version(&self) -> Result<VersionInfo>;
}

#[derive(Debug)]
pub struct DatabaseResponse {
    pub data: Vec<u8>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_length: Option<u64>,
    pub status: StatusCode,
}

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub base_url: String,              // "https://discord.com/api/v9"
    pub endpoint: String,              // "/applications/detectable"
    pub user_agent: String,            // "arRPC-Rust/1.0"
    pub timeout: Duration,             // 30 seconds
    pub connect_timeout: Duration,     // 10 seconds
    pub max_retries: u32,              // 3
    pub retry_delay: Duration,         // 1 second
}
```

### 3. File Manager (`file_manager.rs`)

Handles all file system operations with atomic updates:

```rust
pub struct FileManager {
    database_path: PathBuf,            // Main database file
    temp_dir: PathBuf,                 // Temporary downloads
    backup_dir: PathBuf,               // Backup storage
}

impl FileManager {
    pub fn new(base_path: PathBuf) -> Result<Self>;
    pub async fn write_database_atomic(&self, data: &[u8]) -> Result<()>;
    pub async fn read_current_database(&self) -> Result<Vec<u8>>;
    pub async fn create_backup(&self, label: &str) -> Result<BackupInfo>;
    pub async fn restore_from_backup(&self, backup_id: &str) -> Result<()>;
    pub async fn cleanup_temp_files(&self) -> Result<()>;
    pub fn list_backups(&self) -> Result<Vec<BackupInfo>>;
    pub async fn cleanup_old_backups(&self, keep_count: usize) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct BackupInfo {
    pub id: String,                    // Unique backup identifier
    pub timestamp: SystemTime,         // Creation time
    pub size: u64,                     // File size
    pub checksum: String,              // SHA256 hash
    pub version_info: Option<String>,  // Database version if available
}
```

### 4. Database Validator (`validator.rs`)

Ensures data integrity and schema compliance:

```rust
pub struct DatabaseValidator {
    schema_validator: JsonSchemaValidator,
    integrity_checker: IntegrityChecker,
}

impl DatabaseValidator {
    pub fn new() -> Result<Self>;
    pub async fn validate_raw_data(&self, data: &[u8]) -> Result<ValidationResult>;
    pub async fn validate_parsed_data(&self, games: &[GameEntry]) -> Result<ValidationResult>;
    pub async fn compare_databases(&self, old: &[GameEntry], new: &[GameEntry]) -> Result<ComparisonResult>;
    pub fn validate_single_entry(&self, entry: &GameEntry) -> Result<EntryValidation>;
}

#[derive(Debug)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<ValidationWarning>,
    pub stats: ValidationStats,
}

#[derive(Debug)]
pub struct ValidationStats {
    pub total_games: usize,
    pub linux_games: usize,
    pub windows_games: usize,
    pub macos_games: usize,
    pub games_with_launchers: usize,
    pub unique_executables: usize,
}

#[derive(Debug)]
pub struct ComparisonResult {
    pub added_games: Vec<GameEntry>,
    pub removed_games: Vec<GameEntry>,
    pub modified_games: Vec<GameModification>,
    pub stats_change: StatsChange,
}
```

### 5. Update Scheduler (`scheduler.rs`)

Manages automatic update scheduling and triggers:

```rust
pub struct UpdateScheduler {
    config: SchedulerConfig,
    task_handle: Option<JoinHandle<()>>,
    update_sender: mpsc::Sender<UpdateTrigger>,
    status_receiver: mpsc::Receiver<UpdateStatus>,
}

impl UpdateScheduler {
    pub fn new(config: SchedulerConfig) -> Self;
    pub async fn start(&mut self, manager: Arc<Mutex<DatabaseManager>>) -> Result<()>;
    pub async fn stop(&mut self) -> Result<()>;
    pub async fn trigger_manual_update(&self) -> Result<()>;
    pub fn get_next_scheduled_update(&self) -> Option<SystemTime>;
    pub fn get_last_update_result(&self) -> Option<UpdateResult>;
}

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub enable_auto_updates: bool,
    pub update_interval: Duration,     // Default: 24 hours
    pub max_update_attempts: u32,      // Default: 3
    pub backoff_multiplier: f64,       // Default: 2.0
    pub startup_delay: Duration,       // Default: 5 minutes
}

#[derive(Debug)]
pub enum UpdateTrigger {
    Scheduled,
    Manual,
    Startup,
    ProcessDetectionRequest,
}
```

## Implementation Details

### HTTP Request Handling

```rust
impl HttpClient {
    async fn download_database(&self) -> Result<DatabaseResponse> {
        let url = format!("{}{}", self.config.base_url, self.config.endpoint);
        
        let mut attempt = 0;
        loop {
            attempt += 1;
            
            let request = self.client
                .get(&url)
                .header("User-Agent", &self.config.user_agent)
                .header("Accept", "application/json")
                .timeout(self.config.timeout);
            
            match request.send().await {
                Ok(response) => {
                    match response.status() {
                        StatusCode::OK => {
                            let headers = response.headers().clone();
                            let data = response.bytes().await?;
                            
                            return Ok(DatabaseResponse {
                                data: data.to_vec(),
                                etag: headers.get("etag")
                                    .and_then(|v| v.to_str().ok())
                                    .map(String::from),
                                last_modified: headers.get("last-modified")
                                    .and_then(|v| v.to_str().ok())
                                    .map(String::from),
                                content_length: headers.get("content-length")
                                    .and_then(|v| v.to_str().ok())
                                    .and_then(|v| v.parse().ok()),
                                status: StatusCode::OK,
                            });
                        }
                        StatusCode::NOT_MODIFIED => {
                            return Ok(DatabaseResponse {
                                data: Vec::new(),
                                etag: None,
                                last_modified: None,
                                content_length: Some(0),
                                status: StatusCode::NOT_MODIFIED,
                            });
                        }
                        status => {
                            if attempt >= self.config.max_retries {
                                return Err(DatabaseError::HttpError {
                                    status: status.as_u16(),
                                    message: format!("HTTP {} after {} attempts", status, attempt),
                                });
                            }
                        }
                    }
                }
                Err(e) if attempt >= self.config.max_retries => {
                    return Err(DatabaseError::NetworkError(e));
                }
                Err(_) => {
                    // Retry with exponential backoff
                    let delay = self.config.retry_delay * (2_u32.pow(attempt - 1));
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}
```

### Atomic File Operations

```rust
impl FileManager {
    async fn write_database_atomic(&self, data: &[u8]) -> Result<()> {
        // Create temporary file with unique name
        let temp_id = Uuid::new_v4();
        let temp_path = self.temp_dir.join(format!("detectable_{}.json.tmp", temp_id));
        
        // Write to temporary file
        let mut temp_file = tokio::fs::File::create(&temp_path).await?;
        temp_file.write_all(data).await?;
        temp_file.flush().await?;
        temp_file.sync_all().await?;
        drop(temp_file);
        
        // Verify written data
        let written_data = tokio::fs::read(&temp_path).await?;
        if written_data != data {
            tokio::fs::remove_file(&temp_path).await?;
            return Err(DatabaseError::VerificationFailed);
        }
        
        // Create backup of current database
        if self.database_path.exists() {
            self.create_backup("pre-update").await?;
        }
        
        // Atomic move (rename) to replace current database
        tokio::fs::rename(&temp_path, &self.database_path).await?;
        
        // Verify final file
        let final_data = tokio::fs::read(&self.database_path).await?;
        if final_data != data {
            return Err(DatabaseError::AtomicUpdateFailed);
        }
        
        log::info!("Database updated successfully: {} bytes", data.len());
        Ok(())
    }
}
```

### JSON Schema Validation

```rust
impl DatabaseValidator {
    async fn validate_raw_data(&self, data: &[u8]) -> Result<ValidationResult> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        
        // 1. JSON parsing validation
        let games: Vec<GameEntry> = match serde_json::from_slice(data) {
            Ok(games) => games,
            Err(e) => {
                errors.push(ValidationError::JsonParseError(e.to_string()));
                return Ok(ValidationResult {
                    is_valid: false,
                    errors,
                    warnings,
                    stats: ValidationStats::default(),
                });
            }
        };
        
        // 2. Schema validation
        for (index, game) in games.iter().enumerate() {
            if let Err(validation_errors) = self.validate_single_entry(game) {
                for error in validation_errors {
                    errors.push(ValidationError::SchemaViolation {
                        game_index: index,
                        game_id: game.id.clone(),
                        error: error.to_string(),
                    });
                }
            }
        }
        
        // 3. Business logic validation
        self.validate_business_rules(&games, &mut errors, &mut warnings)?;
        
        // 4. Generate statistics
        let stats = self.calculate_stats(&games);
        
        Ok(ValidationResult {
            is_valid: errors.is_empty(),
            errors,
            warnings,
            stats,
        })
    }
    
    fn validate_business_rules(
        &self,
        games: &[GameEntry],
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) -> Result<()> {
        let mut seen_ids = std::collections::HashSet::new();
        let mut seen_names = std::collections::HashSet::new();
        
        for (index, game) in games.iter().enumerate() {
            // Check for duplicate IDs
            if !seen_ids.insert(&game.id) {
                errors.push(ValidationError::DuplicateId {
                    game_index: index,
                    id: game.id.clone(),
                });
            }
            
            // Check for duplicate names (warning only)
            if !seen_names.insert(&game.name) {
                warnings.push(ValidationWarning::DuplicateName {
                    game_index: index,
                    name: game.name.clone(),
                });
            }
            
            // Validate executables
            if game.executables.is_empty() {
                errors.push(ValidationError::NoExecutables {
                    game_index: index,
                    game_id: game.id.clone(),
                });
            }
            
            // Check for Linux executables (for our use case)
            let has_linux_executable = game.executables
                .iter()
                .any(|exe| exe.os == "linux");
            
            if !has_linux_executable {
                warnings.push(ValidationWarning::NoLinuxExecutable {
                    game_index: index,
                    game_id: game.id.clone(),
                });
            }
        }
        
        Ok(())
    }
}
```

### Database Comparison Logic

```rust
impl DatabaseValidator {
    async fn compare_databases(
        &self,
        old: &[GameEntry],
        new: &[GameEntry],
    ) -> Result<ComparisonResult> {
        let old_map: std::collections::HashMap<&str, &GameEntry> = 
            old.iter().map(|game| (game.id.as_str(), game)).collect();
        let new_map: std::collections::HashMap<&str, &GameEntry> = 
            new.iter().map(|game| (game.id.as_str(), game)).collect();
        
        let mut added_games = Vec::new();
        let mut removed_games = Vec::new();
        let mut modified_games = Vec::new();
        
        // Find added games
        for (id, game) in &new_map {
            if !old_map.contains_key(id) {
                added_games.push((*game).clone());
            }
        }
        
        // Find removed games
        for (id, game) in &old_map {
            if !new_map.contains_key(id) {
                removed_games.push((*game).clone());
            }
        }
        
        // Find modified games
        for (id, new_game) in &new_map {
            if let Some(old_game) = old_map.get(id) {
                if !self.games_equal(old_game, new_game) {
                    modified_games.push(GameModification {
                        id: id.to_string(),
                        old_game: (*old_game).clone(),
                        new_game: (*new_game).clone(),
                        changes: self.identify_changes(old_game, new_game),
                    });
                }
            }
        }
        
        let stats_change = StatsChange {
            old_count: old.len(),
            new_count: new.len(),
            net_change: new.len() as i32 - old.len() as i32,
            added_count: added_games.len(),
            removed_count: removed_games.len(),
            modified_count: modified_games.len(),
        };
        
        Ok(ComparisonResult {
            added_games,
            removed_games,
            modified_games,
            stats_change,
        })
    }
}
```

## Error Handling Strategy

### Custom Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),
    
    #[error("HTTP error: status {status}, message: {message}")]
    HttpError { status: u16, message: String },
    
    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),
    
    #[error("File system error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Validation failed: {0}")]
    ValidationError(String),
    
    #[error("Backup operation failed: {0}")]
    BackupError(String),
    
    #[error("Atomic update verification failed")]
    VerificationFailed,
    
    #[error("Atomic update failed")]
    AtomicUpdateFailed,
    
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    #[error("Scheduler error: {0}")]
    SchedulerError(String),
}

pub type Result<T> = std::result::Result<T, DatabaseError>;
```

### Recovery Strategies

```rust
impl DatabaseManager {
    async fn update_database(&mut self) -> Result<UpdateResult> {
        let mut last_error = None;
        
        for attempt in 1..=self.config.max_update_attempts {
            match self.attempt_update().await {
                Ok(result) => {
                    log::info!("Database update successful on attempt {}", attempt);
                    return Ok(result);
                }
                Err(e) => {
                    last_error = Some(e);
                    log::warn!("Database update attempt {} failed: {:?}", attempt, last_error);
                    
                    if attempt < self.config.max_update_attempts {
                        let delay = Duration::from_secs(2_u64.pow(attempt - 1));
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        
        // All attempts failed - try to recover
        if let Err(recovery_error) = self.attempt_recovery().await {
            log::error!("Recovery failed: {:?}", recovery_error);
        }
        
        Err(last_error.unwrap_or(DatabaseError::ConfigError(
            "Unknown update failure".to_string()
        )))
    }
    
    async fn attempt_recovery(&mut self) -> Result<()> {
        log::info!("Attempting database recovery");
        
        // Try to restore from most recent backup
        let backups = self.backup_manager.list_backups()?;
        if let Some(latest_backup) = backups.first() {
            log::info!("Restoring from backup: {}", latest_backup.id);
            self.file_manager.restore_from_backup(&latest_backup.id).await?;
            return Ok(());
        }
        
        // If no backups available, try to re-download with relaxed validation
        log::warn!("No backups available, attempting emergency download");
        self.emergency_download().await
    }
}
```

## Configuration Management

### Configuration Structure

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub http: HttpConfig,
    pub scheduler: SchedulerConfig,
    pub file_paths: FilePathConfig,
    pub validation: ValidationConfig,
    pub backup: BackupConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilePathConfig {
    pub database_file: PathBuf,        // "./data/detectable.json"
    pub backup_directory: PathBuf,     // "./data/backups"
    pub temp_directory: PathBuf,       // "./data/temp"
    pub log_directory: PathBuf,        // "./logs"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    pub strict_mode: bool,             // Fail on warnings
    pub max_database_size: u64,        // 10MB default
    pub min_game_count: usize,         // 1000 games minimum
    pub require_linux_games: bool,     // Ensure Linux games present
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupConfig {
    pub max_backups: usize,           // Keep 10 backups
    pub compress_backups: bool,        // Use gzip compression
    pub backup_on_update: bool,        // Auto-backup before updates
    pub cleanup_interval: Duration,    // Clean old backups daily
}
```

### Default Configuration

```rust
impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            http: HttpConfig {
                base_url: "https://discord.com/api/v9".to_string(),
                endpoint: "/applications/detectable".to_string(),
                user_agent: "arRPC-Rust/1.0".to_string(),
                timeout: Duration::from_secs(30),
                connect_timeout: Duration::from_secs(10),
                max_retries: 3,
                retry_delay: Duration::from_secs(1),
            },
            scheduler: SchedulerConfig {
                enable_auto_updates: true,
                update_interval: Duration::from_secs(24 * 3600), // 24 hours
                max_update_attempts: 3,
                backoff_multiplier: 2.0,
                startup_delay: Duration::from_secs(5 * 60), // 5 minutes
            },
            file_paths: FilePathConfig {
                database_file: PathBuf::from("./data/detectable.json"),
                backup_directory: PathBuf::from("./data/backups"),
                temp_directory: PathBuf::from("./data/temp"),
                log_directory: PathBuf::from("./logs"),
            },
            validation: ValidationConfig {
                strict_mode: false,
                max_database_size: 10 * 1024 * 1024, // 10MB
                min_game_count: 1000,
                require_linux_games: true,
            },
            backup: BackupConfig {
                max_backups: 10,
                compress_backups: true,
                backup_on_update: true,
                cleanup_interval: Duration::from_secs(24 * 3600),
            },
        }
    }
}
```

## Integration with Process Detection

### Notification System

```rust
pub trait DatabaseUpdateNotifier: Send + Sync {
    async fn on_database_updated(&self, result: &UpdateResult) -> Result<()>;
    async fn on_database_validation_failed(&self, errors: &[ValidationError]) -> Result<()>;
}

impl DatabaseManager {
    pub fn subscribe_to_updates(&mut self, notifier: Arc<dyn DatabaseUpdateNotifier>) {
        self.notifiers.push(notifier);
    }
    
    async fn notify_update_complete(&self, result: &UpdateResult) -> Result<()> {
        for notifier in &self.notifiers {
            if let Err(e) = notifier.on_database_updated(result).await {
                log::warn!("Notifier failed: {:?}", e);
            }
        }
        Ok(())
    }
}
```

### Database Query Interface

```rust
pub struct DatabaseQuery {
    games: Arc<RwLock<Vec<GameEntry>>>,
    linux_executables: Arc<RwLock<HashMap<String, GameEntry>>>,
    name_index: Arc<RwLock<HashMap<String, String>>>, // name -> id mapping
}

impl DatabaseQuery {
    pub async fn find_game_by_executable(&self, executable: &str) -> Option<GameEntry> {
        let executables = self.linux_executables.read().await;
        executables.get(executable).cloned()
    }
    
    pub async fn find_game_by_name(&self, name: &str) -> Option<GameEntry> {
        let name_index = self.name_index.read().await;
        if let Some(id) = name_index.get(name) {
            let games = self.games.read().await;
            return games.iter().find(|g| &g.id == id).cloned();
        }
        None
    }
    
    pub async fn get_linux_games(&self) -> Vec<GameEntry> {
        let games = self.games.read().await;
        games.iter()
            .filter(|game| game.executables.iter().any(|exe| exe.os == "linux"))
            .cloned()
            .collect()
    }
    
    pub async fn reload_from_file(&mut self, database_path: &Path) -> Result<()> {
        let data = tokio::fs::read(database_path).await?;
        let games: Vec<GameEntry> = serde_json::from_slice(&data)?;
        
        // Rebuild indices
        let mut linux_executables = HashMap::new();
        let mut name_index = HashMap::new();
        
        for game in &games {
            // Index by executable name (Linux only)
            for executable in &game.executables {
                if executable.os == "linux" {
                    linux_executables.insert(executable.name.clone(), game.clone());
                }
            }
            
            // Index by name and aliases
            name_index.insert(game.name.clone(), game.id.clone());
            for alias in &game.aliases {
                name_index.insert(alias.clone(), game.id.clone());
            }
        }
        
        *self.games.write().await = games;
        *self.linux_executables.write().await = linux_executables;
        *self.name_index.write().await = name_index;
        
        Ok(())
    }
}
```

## Logging and Monitoring

### Structured Logging

```rust
use tracing::{info, warn, error, debug};

impl DatabaseManager {
    async fn update_database(&mut self) -> Result<UpdateResult> {
        info!("Starting database update");
        
        let start_time = Instant::now();
        let current_stats = self.get_current_stats().await?;
        
        debug!("Current database stats: {:?}", current_stats);
        
        match self.attempt_update().await {
            Ok(result) => {
                let duration = start_time.elapsed();
                info!(
                    duration_ms = duration.as_millis(),
                    games_added = result.comparison.stats_change.added_count,
                    games_removed = result.comparison.stats_change.removed_count,
                    games_modified = result.comparison.stats_change.modified_count,
                    "Database update completed successfully"
                );
                Ok(result)
            }
            Err(e) => {
                error!(
                    error = %e,
                    duration_ms = start_time.elapsed().as_millis(),
                    "Database update failed"
                );
                Err(e)
            }
        }
    }
}
```

### Metrics Collection

```rust
#[derive(Debug, Clone)]
pub struct DatabaseMetrics {
    pub last_update_timestamp: Option<SystemTime>,
    pub last_update_duration: Option<Duration>,
    pub total_update_attempts: u64,
    pub successful_updates: u64,
    pub failed_updates: u64,
    pub current_database_size: u64,
    pub current_game_count: usize,
    pub linux_game_count: usize,
    pub backup_count: usize,
}

impl DatabaseManager {
    pub fn get_metrics(&self) -> DatabaseMetrics {
        self.metrics.clone()
    }
    
    pub async fn export_metrics_json(&self) -> Result<String> {
        let metrics = self.get_metrics();
        serde_json::to_string_pretty(&metrics).map_err(DatabaseError::from)
    }
}
```

## Testing Strategy

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test;
    
    #[tokio::test]
    async fn test_database_validation() {
        let validator = DatabaseValidator::new().unwrap();
        let sample_data = include_bytes!("../test_data/sample_database.json");
        
        let result = validator.validate_raw_data(sample_data).await.unwrap();
        assert!(result.is_valid);
        assert_eq!(result.stats.total_games, 100);
    }
    
    #[tokio::test]
    async fn test_atomic_file_update() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_manager = FileManager::new(temp_dir.path().to_path_buf()).unwrap();
        
        let test_data = b"test database content";
        file_manager.write_database_atomic(test_data).await.unwrap();
        
        let read_data = file_manager.read_current_database().await.unwrap();
        assert_eq!(&read_data[..], test_data);
    }
    
    #[tokio::test]
    async fn test_backup_and_restore() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_manager = FileManager::new(temp_dir.path().to_path_buf()).unwrap();
        
        // Create initial database
        let initial_data = b"initial database";
        file_manager.write_database_atomic(initial_data).await.unwrap();
        
        // Create backup
        let backup_info = file_manager.create_backup("test").await.unwrap();
        
        // Update database
        let updated_data = b"updated database";
        file_manager.write_database_atomic(updated_data).await.unwrap();
        
        // Restore from backup
        file_manager.restore_from_backup(&backup_info.id).await.unwrap();
        
        let restored_data = file_manager.read_current_database().await.unwrap();
        assert_eq!(&restored_data[..], initial_data);
    }
}
```

### Integration Tests

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    use wiremock::{MockServer, Mock, ResponseTemplate};
    
    #[tokio::test]
    async fn test_full_update_cycle() {
        // Mock Discord API
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/applications/detectable"))
            .respond_with(ResponseTemplate::new(200)
                .set_body_json(sample_database_response()))
            .mount(&mock_server)
            .await;
        
        // Configure database manager
        let mut config = DatabaseConfig::default();
        config.http.base_url = mock_server.uri();
        
        let temp_dir = tempfile::tempdir().unwrap();
        config.file_paths.database_file = temp_dir.path().join("detectable.json");
        
        let mut manager = DatabaseManager::new(config).await.unwrap();
        
        // Perform update
        let result = manager.update_database().await.unwrap();
        
        assert!(result.success);
        assert!(result.comparison.stats_change.new_count > 0);
        
        // Verify file was created
        assert!(config.file_paths.database_file.exists());
    }
}
```

## Security Considerations

### Input Validation

- Validate all JSON data against strict schema
- Limit file sizes to prevent DoS attacks
- Sanitize executable paths to prevent directory traversal
- Verify checksums for downloaded data

### Network Security

- Use HTTPS only for API requests
- Implement proper certificate validation
- Add request timeouts to prevent hanging connections
- Rate limiting for API requests

### File System Security

- Use secure temporary file creation
- Implement proper file permissions (600 for sensitive files)
- Validate file paths to prevent directory traversal
- Clean up temporary files on error conditions

## Performance Optimizations

### Memory Management

- Stream large JSON files instead of loading entirely into memory
- Use memory-mapped files for read-only database access
- Implement LRU cache for frequently accessed game entries
- Compress backups to save disk space

### I/O Optimization

- Use async I/O for all file operations
- Batch multiple small operations
- Implement read-ahead caching for database queries
- Use background threads for backup operations

### Network Optimization

- Implement HTTP conditional requests (ETag/If-Modified-Since)
- Use connection pooling for HTTP client
- Implement response compression support
- Cache DNS lookups

## Future Enhancements

1. **Delta Updates**: Only download changes since last update
2. **Distributed Caching**: Share database updates across multiple instances
3. **Custom Game Additions**: Allow users to add custom game definitions
4. **Database Versioning**: Track schema versions for backward compatibility
5. **Real-time Updates**: WebSocket-based real-time database notifications
6. **Multi-source Support**: Support additional game databases beyond Discord
7. **Advanced Analytics**: Track game popularity and usage statistics
8. **Database Sharding**: Split database by categories for better performance

This implementation plan provides a robust, production-ready database management system specifically optimized for Linux environments while maintaining compatibility with the existing arRPC ecosystem.