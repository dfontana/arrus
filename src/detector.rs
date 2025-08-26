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
            .iter()
            .map(|proc| {
                (
                    proc,
                    self.path_processor.process_path(&proc.executable_path),
                )
            })
            .filter_map(|(proc, pinfo)| {
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
        // TODO: How can this be improved? Runtime looks very poor
        // -> Cache DB scan results: https://github.com/OpenAsar/arrpc/pull/123
        // -> Improve linux game detection: https://github.com/OpenAsar/arrpc/pull/143 (this one builds upon https://github.com/OpenAsar/arrpc/pull/92)
        for entry in &db.entries {
            for executable in &entry.executables {
                if executable.is_launcher {
                    // TODO: Verify this logic is intended
                    continue;
                }

                // TODO: Verify this logic is as-intended
                let exe_name = &executable.name;

                // Handle special ">" prefix for process name matching (like >java)
                if let Some(process_name) = exe_name.strip_prefix('>') {
                    // For process name matching, check if the first variant (basename) matches
                    if let Some(first_variant) = path_info.variants.first() {
                        if first_variant != process_name {
                            continue;
                        }
                    } else {
                        continue;
                    }
                } else {
                    // Normal path matching - check if any variant matches the executable name
                    if !path_info.variants.iter().any(|variant| variant == exe_name) {
                        continue;
                    }
                }

                // If arguments are specified, check if process args contain the pattern
                if let Some(required_args) = &executable.arguments {
                    let args_string = args.join(" ");
                    if !args_string.contains(required_args) {
                        continue;
                    }
                }

                return Some(entry);
            }
        }

        None
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
        let mut processes = Vec::new();

        let entries = fs::read_dir(&self.proc_path)?;
        let mut failed_read: usize = 0;

        for entry in entries {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            // Only process numeric directory names (PIDs)
            if let Ok(pid) = file_name_str.parse::<u32>() {
                if let Ok((executable_path, arguments)) = self.read_cmdline(pid) {
                    if !executable_path.is_empty() {
                        processes.push(ProcessInfo {
                            pid,
                            executable_path,
                            arguments,
                        });
                    }
                } else {
                    // Silently ignore processes we can't read (permissions, etc.)
                    failed_read += 1;
                }
            }
        }

        debug!(
            "Scanner found {} processes, failed to parse {}",
            processes.len(),
            failed_read
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

        let mut variants = Vec::new();

        // Generate suffix combinations (like the Node.js implementation)
        for i in 1..split_path.len() {
            let suffix = split_path[split_path.len() - i..].join("/");
            if !suffix.is_empty() {
                variants.push(suffix);
            }
        }

        // Create variants with 64-bit identifiers removed
        let original_variants = variants.clone();
        for variant in original_variants {
            let mut cleaned = variant.clone();

            // Remove various 64-bit patterns (matching Node.js logic)
            cleaned = cleaned.replace("64", "");
            if cleaned != variant {
                variants.push(cleaned.clone());
            }

            cleaned = variant.replace(".x64", "");
            if cleaned != variant {
                variants.push(cleaned.clone());
            }

            cleaned = variant.replace("x64", "");
            if cleaned != variant {
                variants.push(cleaned.clone());
            }

            cleaned = variant.replace("_64", "");
            if cleaned != variant {
                variants.push(cleaned);
            }
        }

        // Remove duplicates while preserving order
        let mut seen = std::collections::HashSet::new();
        variants.retain(|v| seen.insert(v.clone()));
        ProcessedPath { variants }
    }
}
