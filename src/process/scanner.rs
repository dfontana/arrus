use std::fs;
use std::path::PathBuf;

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
                }
                // Silently ignore processes we can't read (permissions, etc.)
            }
        }

        Ok(processes)
    }

    fn read_cmdline(&self, pid: u32) -> Result<(String, Vec<String>), anyhow::Error> {
        let cmdline_path = self.proc_path.join(pid.to_string()).join("cmdline");
        let content = fs::read_to_string(&cmdline_path)?;
        let (executable, args) = self.parse_cmdline(&content);
        Ok((executable, args))
    }

    fn parse_cmdline(&self, content: &str) -> (String, Vec<String>) {
        let parts: Vec<&str> = content.split('\0').collect();

        if parts.is_empty() || parts[0].is_empty() {
            return (String::new(), Vec::new());
        }

        let executable = parts[0].to_string();
        let arguments = parts
            .iter()
            .skip(1)
            .filter(|&s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        (executable, arguments)
    }
}

impl Default for ProcessScanner {
    fn default() -> Self {
        Self::new()
    }
}
