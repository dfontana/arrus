use crate::activity::ActivityMessage;
use crate::process::{
    activity_manager::ActivityManager,
    database::GameDatabase,
    error::DetectorError,
    path_processor::PathProcessor,
    scanner::ProcessScanner,
};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub struct ProcessDetector {
    scanner: ProcessScanner,
    path_processor: PathProcessor,
    database: GameDatabase,
    activity_manager: ActivityManager,
    scan_interval: Duration,
}

impl ProcessDetector {
    pub fn new<P: AsRef<Path>>(
        database_path: P,
        message_sender: mpsc::UnboundedSender<ActivityMessage>,
    ) -> Result<Self, DetectorError> {
        let database = GameDatabase::load_from_file(database_path)?;

        tracing::info!(
            "Loaded game database with {} total games, {} Linux-compatible",
            database.len(),
            database.linux_len()
        );

        Ok(Self {
            scanner: ProcessScanner::new(),
            path_processor: PathProcessor::new(),
            database,
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

            match self.scan_cycle() {
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

    fn scan_cycle(&mut self) -> Result<ScanStats, DetectorError> {
        let start_time = std::time::Instant::now();

        // Get all running processes
        let processes = self.scanner.scan_processes()?;
        let process_count = processes.len();

        // Find matching games
        let mut detected_games = Vec::new();

        for process in processes {
            let path_info = self.path_processor.process_path(&process.executable_path);

            if let Some(game) = self.database.find_match(&path_info, &process.arguments) {
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

    pub fn set_scan_interval(&mut self, interval: Duration) {
        self.scan_interval = interval;
    }
}

#[derive(Debug)]
struct ScanStats {
    processes_scanned: usize,
    games_detected: usize,
    scan_duration: Duration,
}
