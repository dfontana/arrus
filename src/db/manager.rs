use crate::db::{
    error::{DatabaseError, Result},
    file_manager::{BackupInfo, FileManager},
    http_client::{HttpClient, HttpConfig},
    scheduler::{SchedulerConfig, UpdateResult, UpdateScheduler, UpdateTrigger},
    validator::{ComparisonResult, DatabaseValidator, ValidationConfig, ValidationResult},
};
use crate::process::database::GameEntry;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub http: HttpConfig,
    pub scheduler: SchedulerConfig,
    pub validation: ValidationConfig,
    pub file_paths: FilePathConfig,
    pub backup: BackupConfig,
}

#[derive(Debug, Clone)]
pub struct FilePathConfig {
    pub database_file: PathBuf,
    pub backup_directory: PathBuf,
    pub temp_directory: PathBuf,
}

#[derive(Debug, Clone)]
pub struct BackupConfig {
    pub max_backups: usize,
    pub compress_backups: bool,
    pub backup_on_update: bool,
    pub cleanup_interval: Duration,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            http: HttpConfig::default(),
            scheduler: SchedulerConfig::default(),
            validation: ValidationConfig::default(),
            file_paths: FilePathConfig {
                database_file: PathBuf::from("./data/detectable.json"),
                backup_directory: PathBuf::from("./data/backups"),
                temp_directory: PathBuf::from("./data/temp"),
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

#[derive(Debug, Clone)]
pub struct DatabaseInfo {
    pub path: PathBuf,
    pub exists: bool,
    pub size: Option<u64>,
    pub last_modified: Option<SystemTime>,
    pub game_count: Option<usize>,
    pub linux_game_count: Option<usize>,
    pub last_update: Option<SystemTime>,
}

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

pub struct DatabaseManager {
    config: DatabaseConfig,
    http_client: HttpClient,
    file_manager: FileManager,
    validator: DatabaseValidator,
    scheduler: UpdateScheduler,
    metrics: Arc<RwLock<DatabaseMetrics>>,
    current_etag: Arc<Mutex<Option<String>>>,
}

impl DatabaseManager {
    pub async fn new(config: DatabaseConfig) -> Result<Self> {
        let http_client = HttpClient::new(config.http.clone())?;
        let file_manager = FileManager::new(
            config
                .file_paths
                .database_file
                .parent()
                .unwrap()
                .to_path_buf(),
        )?;
        let validator = DatabaseValidator::new(config.validation.clone());
        let scheduler = UpdateScheduler::new(config.scheduler.clone());

        let metrics = Arc::new(RwLock::new(DatabaseMetrics {
            last_update_timestamp: None,
            last_update_duration: None,
            total_update_attempts: 0,
            successful_updates: 0,
            failed_updates: 0,
            current_database_size: 0,
            current_game_count: 0,
            linux_game_count: 0,
            backup_count: 0,
        }));

        Ok(Self {
            config,
            http_client,
            file_manager,
            validator,
            scheduler,
            metrics,
            current_etag: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn initialize(&mut self) -> Result<()> {
        // Update initial metrics
        self.update_metrics().await?;

        // We'll start the scheduler later since we need to avoid self-reference
        tracing::info!("Database manager initialized (scheduler will be started separately)");
        Ok(())
    }

    pub async fn start_scheduler(&mut self) -> Result<()> {
        // Create a simple callback that just logs - in a real implementation,
        // we would need a different architecture to avoid self-reference
        let update_callback = |trigger: UpdateTrigger| async move {
            tracing::info!("Update triggered: {:?}", trigger);
            // This is a placeholder - in practice we need a different design
            // to avoid self-referential issues
            Ok(UpdateResult {
                trigger,
                started_at: SystemTime::now(),
                completed_at: SystemTime::now(),
                success: false,
                games_added: 0,
                games_removed: 0,
                games_modified: 0,
                error: Some("Not implemented yet".to_string()),
            })
        };

        self.scheduler.start(update_callback).await?;

        // Trigger startup update if database doesn't exist
        if !self.file_manager.database_exists() {
            tracing::info!("Database doesn't exist, triggering startup update");
            self.scheduler.trigger_startup_update().await?;
        }

        tracing::info!("Database manager scheduler started");
        Ok(())
    }

    pub async fn update_database(&mut self) -> Result<UpdateResult> {
        self.perform_update(UpdateTrigger::Manual).await
    }

    pub async fn force_update(&mut self) -> Result<UpdateResult> {
        // Clear etag to force full download
        *self.current_etag.lock().await = None;
        self.perform_update(UpdateTrigger::Manual).await
    }

    async fn perform_update(&mut self, trigger: UpdateTrigger) -> Result<UpdateResult> {
        let start_time = Instant::now();
        let started_at = SystemTime::now();

        tracing::info!("Starting database update (trigger: {:?})", trigger);

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.total_update_attempts += 1;
        }

        let mut attempt = 0;
        let mut last_error = None;

        while attempt < self.config.scheduler.max_update_attempts {
            attempt += 1;

            match self.attempt_update().await {
                Ok(comparison) => {
                    let completed_at = SystemTime::now();
                    let duration = start_time.elapsed();

                    // Update metrics
                    {
                        let mut metrics = self.metrics.write().await;
                        metrics.successful_updates += 1;
                        metrics.last_update_timestamp = Some(completed_at);
                        metrics.last_update_duration = Some(duration);
                    }

                    self.update_metrics().await?;

                    let result = UpdateResult {
                        trigger,
                        started_at,
                        completed_at,
                        success: true,
                        games_added: comparison.stats_change.added_count,
                        games_removed: comparison.stats_change.removed_count,
                        games_modified: comparison.stats_change.modified_count,
                        error: None,
                    };

                    tracing::info!(
                        "Database update completed successfully in {:?} (attempt {}): +{} -{} ~{}",
                        duration,
                        attempt,
                        result.games_added,
                        result.games_removed,
                        result.games_modified
                    );

                    return Ok(result);
                }
                Err(e) => {
                    last_error = Some(e);
                    tracing::warn!(
                        "Database update attempt {} failed: {:?}",
                        attempt,
                        last_error
                    );

                    if attempt < self.config.scheduler.max_update_attempts {
                        let delay = Duration::from_secs(
                            (self
                                .config
                                .scheduler
                                .backoff_multiplier
                                .powi(attempt as i32 - 1)) as u64,
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        // All attempts failed
        {
            let mut metrics = self.metrics.write().await;
            metrics.failed_updates += 1;
        }

        let error_msg = last_error
            .map(|e| e.to_string())
            .unwrap_or_else(|| "Unknown update failure".to_string());

        // Try recovery
        if let Err(recovery_error) = self.attempt_recovery().await {
            tracing::error!("Recovery failed: {:?}", recovery_error);
        }

        let _result = UpdateResult {
            trigger,
            started_at,
            completed_at: SystemTime::now(),
            success: false,
            games_added: 0,
            games_removed: 0,
            games_modified: 0,
            error: Some(error_msg.clone()),
        };

        Err(DatabaseError::ConfigError(error_msg))
    }

    async fn attempt_update(&mut self) -> Result<ComparisonResult> {
        // Get current etag for conditional request
        let etag = self.current_etag.lock().await.clone();

        // Download database
        let response = self.http_client.download_with_etag(etag.as_deref()).await?;

        if response.status == reqwest::StatusCode::NOT_MODIFIED {
            tracing::info!("Database not modified (304), skipping update");
            return Ok(ComparisonResult {
                added_games: Vec::new(),
                removed_games: Vec::new(),
                modified_games: Vec::new(),
                stats_change: crate::db::validator::StatsChange {
                    old_count: 0,
                    new_count: 0,
                    net_change: 0,
                    added_count: 0,
                    removed_count: 0,
                    modified_count: 0,
                },
            });
        }

        // Validate downloaded data
        let validation_result = self.validator.validate_raw_data(&response.data).await?;
        if !validation_result.is_valid {
            return Err(DatabaseError::ValidationError(format!(
                "Downloaded database validation failed: {:?}",
                validation_result.errors
            )));
        }

        // Parse new data
        let new_games: Vec<GameEntry> = serde_json::from_slice(&response.data)?;

        // Compare with existing database if it exists
        let comparison = if self.file_manager.database_exists() {
            let current_data = self.file_manager.read_current_database().await?;
            let current_games: Vec<GameEntry> = serde_json::from_slice(&current_data)?;
            self.validator
                .compare_databases(&current_games, &new_games)
                .await?
        } else {
            ComparisonResult {
                added_games: new_games.clone(),
                removed_games: Vec::new(),
                modified_games: Vec::new(),
                stats_change: crate::db::validator::StatsChange {
                    old_count: 0,
                    new_count: new_games.len(),
                    net_change: new_games.len() as i32,
                    added_count: new_games.len(),
                    removed_count: 0,
                    modified_count: 0,
                },
            }
        };

        // Write new database atomically
        self.file_manager
            .write_database_atomic(&response.data)
            .await?;

        // Update etag
        if let Some(etag) = response.etag {
            *self.current_etag.lock().await = Some(etag);
        }

        // Cleanup old backups
        self.file_manager
            .cleanup_old_backups(self.config.backup.max_backups)
            .await?;

        Ok(comparison)
    }

    async fn attempt_recovery(&mut self) -> Result<()> {
        tracing::info!("Attempting database recovery");

        // Try to restore from most recent backup
        let backups = self.file_manager.list_backups()?;
        if let Some(latest_backup) = backups.first() {
            tracing::info!("Restoring from backup: {}", latest_backup.id);
            self.file_manager
                .restore_from_backup(&latest_backup.id)
                .await?;
            return Ok(());
        }

        tracing::warn!("No backups available for recovery");
        Err(DatabaseError::BackupError(
            "No backups available".to_string(),
        ))
    }

    pub async fn rollback_to_backup(&mut self, backup_id: &str) -> Result<()> {
        self.file_manager.restore_from_backup(backup_id).await?;
        self.update_metrics().await?;
        tracing::info!("Rolled back to backup: {}", backup_id);
        Ok(())
    }

    pub async fn validate_current_database(&self) -> Result<ValidationResult> {
        if !self.file_manager.database_exists() {
            return Err(DatabaseError::ValidationError(
                "Database file does not exist".to_string(),
            ));
        }

        let data = self.file_manager.read_current_database().await?;
        self.validator.validate_raw_data(&data).await
    }

    pub async fn get_database_info(&self) -> DatabaseInfo {
        let path = self.file_manager.get_database_path().to_path_buf();
        let exists = self.file_manager.database_exists();

        let (size, last_modified, game_count, linux_game_count) = if exists {
            match tokio::fs::metadata(&path).await {
                Ok(metadata) => {
                    let size = Some(metadata.len());
                    let last_modified = metadata.modified().ok();

                    let (game_count, linux_game_count) =
                        match self.file_manager.read_current_database().await {
                            Ok(data) => match serde_json::from_slice::<Vec<GameEntry>>(&data) {
                                Ok(games) => {
                                    let total = games.len();
                                    let linux = games
                                        .iter()
                                        .filter(|g| g.executables.iter().any(|e| e.os == "linux"))
                                        .count();
                                    (Some(total), Some(linux))
                                }
                                Err(_) => (None, None),
                            },
                            Err(_) => (None, None),
                        };

                    (size, last_modified, game_count, linux_game_count)
                }
                Err(_) => (None, None, None, None),
            }
        } else {
            (None, None, None, None)
        };

        DatabaseInfo {
            path,
            exists,
            size,
            last_modified,
            game_count,
            linux_game_count,
            last_update: self.metrics.read().await.last_update_timestamp,
        }
    }

    pub async fn get_metrics(&self) -> DatabaseMetrics {
        self.metrics.read().await.clone()
    }

    pub async fn list_backups(&self) -> Result<Vec<BackupInfo>> {
        self.file_manager.list_backups()
    }

    pub async fn create_manual_backup(&self, label: &str) -> Result<BackupInfo> {
        self.file_manager.create_backup(label).await
    }

    pub async fn trigger_manual_update(&self) -> Result<()> {
        self.scheduler.trigger_manual_update().await
    }

    pub fn get_next_scheduled_update(&self) -> Option<SystemTime> {
        self.scheduler.get_next_scheduled_update()
    }

    pub async fn cleanup_temp_files(&self) -> Result<()> {
        self.file_manager.cleanup_temp_files().await
    }

    async fn update_metrics(&self) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        let info = self.get_database_info().await;

        metrics.current_database_size = info.size.unwrap_or(0);
        metrics.current_game_count = info.game_count.unwrap_or(0);
        metrics.linux_game_count = info.linux_game_count.unwrap_or(0);
        metrics.backup_count = self.file_manager.list_backups().unwrap_or_default().len();

        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.scheduler.stop().await?;
        self.cleanup_temp_files().await?;
        tracing::info!("Database manager shut down");
        Ok(())
    }
}
