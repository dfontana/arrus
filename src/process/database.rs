use crate::process::error::DatabaseError;
use crate::process::path_processor::ProcessedPath;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GameEntry {
    pub id: String,
    pub name: String,
    pub executables: Vec<ExecutableEntry>,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub hook: bool,
    #[serde(default)]
    pub overlay: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExecutableEntry {
    pub name: String,
    pub os: String,
    #[serde(default)]
    pub is_launcher: bool,
    pub arguments: Option<String>,
}

pub struct GameDatabase {
    entries: Vec<GameEntry>,
    linux_entries: Vec<GameEntry>,
}

impl GameDatabase {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, DatabaseError> {
        let path_ref = path.as_ref();
        let content = fs::read_to_string(path_ref).map_err(|e| DatabaseError::LoadError {
            path: path_ref.display().to_string(),
            source: serde_json::Error::io(e),
        })?;

        let entries: Vec<GameEntry> =
            serde_json::from_str(&content).map_err(|e| DatabaseError::LoadError {
                path: path_ref.display().to_string(),
                source: e,
            })?;

        // Pre-filter Linux-compatible entries for performance
        let linux_entries = entries
            .iter()
            .filter(|entry| entry.executables.iter().any(|exec| exec.os == "linux"))
            .cloned()
            .collect();

        Ok(Self {
            entries,
            linux_entries,
        })
    }

    pub fn find_match(&self, path_info: &ProcessedPath, args: &[String]) -> Option<&GameEntry> {
        // Only search Linux-compatible games
        for entry in &self.linux_entries {
            for executable in &entry.executables {
                // Skip non-Linux executables
                if executable.os != "linux" {
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

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn linux_len(&self) -> usize {
        self.linux_entries.len()
    }
}
