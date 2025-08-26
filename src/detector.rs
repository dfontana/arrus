use crate::activity::{ActivityManager, ActivityMessage};
use crate::database::{DatabaseConfig, GameDatabase, GameEntry, store};
use kitchen_sink::simple_store::Store;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, error, instrument};

pub struct ProcessDetector {
    database: Store<GameDatabase>,
    scanner: ProcessScanner,
    path_processor: PathProcessor,
    activity_manager: ActivityManager,
    scan_interval: Duration,
}

impl ProcessDetector {
    pub async fn new_with_manager(
        message_sender: broadcast::Sender<ActivityMessage>,
        config: DatabaseConfig,
    ) -> Result<Self, anyhow::Error> {
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

    #[instrument(skip(self))]
    async fn scan_cycle(&mut self) -> Result<(), anyhow::Error> {
        let start_time = Instant::now();

        let matcher = Matcher {};

        let database = self.database.read();
        let detected_games = self
            .scanner
            .scan_processes()?
            .into_iter()
            .filter_map(|proc| {
                let pinfo = self.path_processor.process_path(&proc.executable_path);
                matcher
                    .find_match(&database, &pinfo, &proc.arguments)
                    .map(|game| (game, proc.pid))
            })
            .collect();

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
        db.entries.iter().find_map(|entry| {
            entry
                .executables
                .iter()
                .filter(|exe| !exe.is_launcher)
                .find(|exe| self.matches_executable(exe, path_info, args))
                .map(|_| entry)
        })
    }

    fn matches_executable(
        &self,
        executable: &crate::database::ExecutableEntry,
        path_info: &ProcessedPath,
        args: &[String],
    ) -> bool {
        let exe_name = &executable.name;

        let path_matches = if let Some(process_name) = exe_name.strip_prefix('>') {
            path_info
                .variants
                .first()
                .is_some_and(|first_variant| first_variant == process_name)
        } else {
            path_info.variants.iter().any(|variant| variant == exe_name)
        };

        if !path_matches {
            return false;
        }

        executable.arguments.as_ref().is_none_or(|required_args| {
            let args_string = args.join(" ");
            args_string.contains(required_args)
        })
    }
}

#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub pid: u32,
    pub executable_path: String,
    pub arguments: Vec<String>,
}

pub struct ProcessScanner {
    proc_path: PathBuf,
}

impl ProcessScanner {
    pub fn new() -> Self {
        Self {
            proc_path: PathBuf::from("/proc"),
        }
    }

    pub fn scan_processes(&self) -> Result<Vec<ProcessInfo>, anyhow::Error> {
        let entries = fs::read_dir(&self.proc_path)?;

        let (processes, failed_count): (Vec<_>, usize) = entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .parse::<u32>()
                    .ok()
                    .map(|pid| (pid, entry))
            })
            .fold((Vec::new(), 0), |(mut processes, mut failed), (pid, _)| {
                match self.read_cmdline(pid) {
                    Ok((executable_path, arguments)) if !executable_path.is_empty() => {
                        processes.push(ProcessInfo {
                            pid,
                            executable_path,
                            arguments,
                        });
                    }
                    Ok(_) => {} // Empty executable path, ignore
                    Err(_) => failed += 1,
                }
                (processes, failed)
            });

        debug!(
            "Scanner found {} processes, failed to parse {}",
            processes.len(),
            failed_count
        );

        Ok(processes)
    }

    fn read_cmdline(&self, pid: u32) -> Result<(String, Vec<String>), anyhow::Error> {
        let cmdline_path = self.proc_path.join(pid.to_string()).join("cmdline");
        let content = fs::read_to_string(&cmdline_path)?;
        let parts: Vec<&str> = content.split('\0').collect();

        if parts.is_empty() || parts[0].is_empty() {
            return Ok((String::new(), Vec::new()));
        }

        let executable = parts[0].to_string();
        let args = parts
            .iter()
            .skip(1)
            .filter(|&s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        Ok((executable, args))
    }
}

// Detect across paths?

#[derive(Debug, Clone)]
pub struct ProcessedPath {
    pub variants: Vec<String>,
}

pub struct PathProcessor;

impl PathProcessor {
    pub fn new() -> Self {
        Self
    }

    pub fn process_path(&self, path: &str) -> ProcessedPath {
        let normalized = path.to_lowercase().replace('\\', "/");
        let split_path: Vec<&str> = normalized.split('/').collect();

        let base_variants = (1..split_path.len()).filter_map(|i| {
            let suffix = split_path[split_path.len() - i..].join("/");
            if suffix.is_empty() {
                None
            } else {
                Some(suffix)
            }
        });

        let mut variants: Vec<String> = base_variants
            .flat_map(|variant| {
                std::iter::once(variant.clone()).chain(Self::generate_cleaned_variants(&variant))
            })
            .collect();

        // Remove duplicates while preserving order
        let mut seen = std::collections::HashSet::new();
        variants.retain(|v| seen.insert(v.clone()));

        ProcessedPath { variants }
    }

    fn generate_cleaned_variants(variant: &str) -> Vec<String> {
        let patterns = ["64", ".x64", "x64", "_64"];

        patterns
            .iter()
            .filter_map(|&pattern| {
                let cleaned = variant.replace(pattern, "");
                if cleaned != variant && !cleaned.is_empty() {
                    Some(cleaned)
                } else {
                    None
                }
            })
            .collect()
    }
}
