mod activity_manager;
mod path_processor;
mod scanner;

use crate::activity::ActivityMessage;
use crate::database::{
    DatabaseConfig, ExecutableEntry, GameDatabase, GameEntry, OperatingSystem, store,
};
use activity_manager::ActivityManager;
use kitchen_sink::simple_store::Store;
use path_processor::{PathProcessor, ProcessedPath};
use scanner::ProcessScanner;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error};

pub struct ProcessDetector {
    database: Store<GameDatabase>,
    scanner: ProcessScanner,
    path_processor: PathProcessor,
    activity_manager: ActivityManager,
    scan_interval: Duration,
}

impl ProcessDetector {
    pub async fn new_with_manager(
        message_sender: mpsc::UnboundedSender<ActivityMessage>,
        db_config: Option<DatabaseConfig>,
    ) -> Result<Self, anyhow::Error> {
        let config = db_config.unwrap_or_default();
        let database = store(config.clone()).await?;
        Ok(Self {
            database,
            scanner: ProcessScanner::new(),
            path_processor: PathProcessor::new(),
            activity_manager: ActivityManager::new(message_sender),
            scan_interval: Duration::from_secs(5),
        })
    }

    pub fn start(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.scan_interval);
            loop {
                interval.tick().await;
                if let Err(e) = self.scan_cycle().await {
                    error!("Scan cycle failed: {}", e);
                }
            }
        })
    }

    async fn scan_cycle(&mut self) -> Result<(), anyhow::Error> {
        let start_time = Instant::now();

        // Get all running processes
        let processes = self.scanner.scan_processes()?;

        // Find matching games
        let mut detected_games = Vec::new();
        let database = self.database.read();
        let matcher = Matcher {};

        for process in processes {
            let path_info = self.path_processor.process_path(&process.executable_path);
            if let Some(game) = matcher.find_match(&database, &path_info, &process.arguments) {
                detected_games.push((game, process.pid));
            }
        }

        // Update activity manager with detected games
        self.activity_manager.update_detected_games(detected_games);

        let duration = start_time.elapsed();
        debug!("Scan took {:?}", duration);

        Ok(())
    }
}

struct Matcher;
impl Matcher {
    pub fn find_match<'a>(
        &self,
        db: &'a GameDatabase,
        path_info: &ProcessedPath,
        args: &[String],
    ) -> Option<&'a GameEntry> {
        // TODO: Why?
        // Only search Linux-compatible games
        for entry in &db.linux_entries {
            for executable in &entry.executables {
                // Skip non-Linux executables
                if executable.os != OperatingSystem::Linux {
                    continue;
                }

                // Skip launchers
                if executable.is_launcher {
                    continue;
                }

                if self.is_executable_match(executable, &path_info.variants, args) {
                    return Some(entry);
                }
            }
        }

        None
    }

    fn is_executable_match(
        &self,
        executable: &ExecutableEntry,
        variants: &[String],
        args: &[String],
    ) -> bool {
        let exe_name = &executable.name;

        // Handle special ">" prefix for process name matching (like >java)
        if let Some(process_name) = exe_name.strip_prefix('>') {
            // For process name matching, check if the first variant (basename) matches
            if let Some(first_variant) = variants.first() {
                if first_variant != process_name {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            // Normal path matching - check if any variant matches the executable name
            if !variants.iter().any(|variant| variant == exe_name) {
                return false;
            }
        }

        // If arguments are specified, check if process args contain the pattern
        if let Some(required_args) = &executable.arguments {
            let args_string = args.join(" ");
            if !args_string.contains(required_args) {
                return false;
            }
        }

        true
    }
}
