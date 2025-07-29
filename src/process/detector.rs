use crate::activity::ActivityMessage;
use crate::db::{DatabaseConfig, DatabaseManager};
use crate::process::{
    activity_manager::ActivityManager, database::GameDatabase, error::DetectorError,
    path_processor::PathProcessor, scanner::ProcessScanner,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;

pub struct ProcessDetector {
    scanner: ProcessScanner,
    path_processor: PathProcessor,
    database: Arc<RwLock<GameDatabase>>,
    activity_manager: ActivityManager,
    scan_interval: Duration,
}

impl ProcessDetector {
    pub async fn new_with_manager(
        message_sender: mpsc::UnboundedSender<ActivityMessage>,
        db_config: Option<DatabaseConfig>,
    ) -> Result<Self, DetectorError> {
        let config = db_config.unwrap_or_default();

        // Create database manager
        let mut db_manager = DatabaseManager::new(config.clone())
            .await
            .map_err(|e| DetectorError::Database(e.into()))?;

        // Initialize database manager
        db_manager
            .initialize()
            .await
            .map_err(|e| DetectorError::Database(e.into()))?;

        // Load initial database
        let database = if db_manager.get_database_info().await.exists {
            GameDatabase::load_from_file(&config.file_paths.database_file)?
        } else {
            // Wait for initial download
            tokio::time::sleep(Duration::from_secs(10)).await;
            if db_manager.get_database_info().await.exists {
                GameDatabase::load_from_file(&config.file_paths.database_file)?
            } else {
                return Err(DetectorError::Database(
                    crate::process::error::DatabaseError::LoadError {
                        path: config.file_paths.database_file.display().to_string(),
                        source: serde_json::Error::io(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Database not available after initialization",
                        )),
                    },
                ));
            }
        };

        tracing::info!(
            "Loaded game database with {} total games, {} Linux-compatible",
            database.len(),
            database.linux_len()
        );

        Ok(Self {
            scanner: ProcessScanner::new(),
            path_processor: PathProcessor::new(),
            database: Arc::new(RwLock::new(database)),
            activity_manager: ActivityManager::new(message_sender),
            scan_interval: Duration::from_secs(5),
        })
    }

    pub fn start(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.run_detection_loop().await;
        })
    }

    async fn run_detection_loop(&mut self) {
        let mut interval = tokio::time::interval(self.scan_interval);
        tracing::info!(
            "Process detection started, scanning every {:?}",
            self.scan_interval
        );

        loop {
            interval.tick().await;

            match self.scan_cycle().await {
                Ok(stats) => {
                    tracing::debug!(
                        "Scan completed: {} processes, {} games detected",
                        stats.processes_scanned,
                        stats.games_detected
                    );
                }
                Err(e) => {
                    tracing::error!("Scan cycle failed: {}", e);
                    // Continue running despite errors
                }
            }
        }
    }

    async fn scan_cycle(&mut self) -> Result<ScanStats, DetectorError> {
        let start_time = std::time::Instant::now();

        // Get all running processes
        let processes = self.scanner.scan_processes()?;
        let process_count = processes.len();

        // Find matching games
        let mut detected_games = Vec::new();
        let database = self.database.read().await;

        for process in processes {
            let path_info = self.path_processor.process_path(&process.executable_path);

            if let Some(game) = database.find_match(&path_info, &process.arguments) {
                detected_games.push((game, process.pid));
            }
        }

        let game_count = detected_games.len();

        // Update activity manager with detected games
        self.activity_manager.update_detected_games(detected_games);

        let duration = start_time.elapsed();
        tracing::debug!("Scan took {:?}", duration);

        Ok(ScanStats {
            processes_scanned: process_count,
            games_detected: game_count,
            scan_duration: duration,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct ScanStats {
    processes_scanned: usize,
    games_detected: usize,
    scan_duration: Duration,
}
