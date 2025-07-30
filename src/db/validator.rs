use crate::db::error::Result;
use crate::process::database::{ExecutableEntry, GameEntry};
use std::collections::{HashMap, HashSet};

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

impl Default for ValidationStats {
    fn default() -> Self {
        Self {
            total_games: 0,
            linux_games: 0,
            windows_games: 0,
            macos_games: 0,
            games_with_launchers: 0,
            unique_executables: 0,
        }
    }
}

#[derive(Debug)]
pub enum ValidationError {
    JsonParseError(String),
    SchemaViolation {
        game_index: usize,
        game_id: String,
        error: String,
    },
    DuplicateId {
        game_index: usize,
        id: String,
    },
    NoExecutables {
        game_index: usize,
        game_id: String,
    },
    InvalidExecutable {
        game_index: usize,
        game_id: String,
        executable_index: usize,
        error: String,
    },
}

#[derive(Debug)]
pub enum ValidationWarning {
    DuplicateName { game_index: usize, name: String },
    NoLinuxExecutable { game_index: usize, game_id: String },
    EmptyName { game_index: usize, game_id: String },
}

#[derive(Debug)]
pub struct ComparisonResult {
    pub added_games: Vec<GameEntry>,
    pub removed_games: Vec<GameEntry>,
    pub modified_games: Vec<GameModification>,
    pub stats_change: StatsChange,
}

#[derive(Debug)]
pub struct GameModification {
    pub id: String,
    pub old_game: GameEntry,
    pub new_game: GameEntry,
    pub changes: Vec<String>,
}

#[derive(Debug)]
pub struct StatsChange {
    pub old_count: usize,
    pub new_count: usize,
    pub net_change: i32,
    pub added_count: usize,
    pub removed_count: usize,
    pub modified_count: usize,
}

#[derive(Debug, Clone)]
pub struct ValidationConfig {
    pub strict_mode: bool,
    pub max_database_size: u64,
    pub min_game_count: usize,
    pub require_linux_games: bool,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            strict_mode: false,
            max_database_size: 10 * 1024 * 1024, // 10MB
            min_game_count: 1000,
            require_linux_games: true,
        }
    }
}

pub struct DatabaseValidator {
    config: ValidationConfig,
}

impl DatabaseValidator {
    pub fn new(config: ValidationConfig) -> Self {
        Self { config }
    }

    pub async fn validate_raw_data(&self, data: &[u8]) -> Result<ValidationResult> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Check database size
        if data.len() as u64 > self.config.max_database_size {
            errors.push(ValidationError::SchemaViolation {
                game_index: 0,
                game_id: "global".to_string(),
                error: format!(
                    "Database size {} exceeds maximum {}",
                    data.len(),
                    self.config.max_database_size
                ),
            });
        }

        // JSON parsing validation
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

        // Check minimum game count
        if games.len() < self.config.min_game_count {
            errors.push(ValidationError::SchemaViolation {
                game_index: 0,
                game_id: "global".to_string(),
                error: format!(
                    "Game count {} below minimum {}",
                    games.len(),
                    self.config.min_game_count
                ),
            });
        }

        // Schema validation
        for (index, game) in games.iter().enumerate() {
            self.validate_single_entry(game, index, &mut errors, &mut warnings);
        }

        // Business logic validation
        self.validate_business_rules(&games, &mut errors, &mut warnings);

        // Generate statistics
        let stats = self.calculate_stats(&games);

        // Check Linux games requirement
        if self.config.require_linux_games && stats.linux_games == 0 {
            errors.push(ValidationError::SchemaViolation {
                game_index: 0,
                game_id: "global".to_string(),
                error: "No Linux games found in database".to_string(),
            });
        }

        Ok(ValidationResult {
            is_valid: errors.is_empty() && (!self.config.strict_mode || warnings.is_empty()),
            errors,
            warnings,
            stats,
        })
    }

    pub async fn validate_parsed_data(&self, games: &[GameEntry]) -> Result<ValidationResult> {
        let data = serde_json::to_vec(games)?;
        self.validate_raw_data(&data).await
    }

    pub async fn compare_databases(
        &self,
        old: &[GameEntry],
        new: &[GameEntry],
    ) -> Result<ComparisonResult> {
        let old_map: HashMap<&str, &GameEntry> =
            old.iter().map(|game| (game.id.as_str(), game)).collect();
        let new_map: HashMap<&str, &GameEntry> =
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

    fn validate_single_entry(
        &self,
        game: &GameEntry,
        index: usize,
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) {
        // Check required fields
        if game.id.is_empty() {
            errors.push(ValidationError::SchemaViolation {
                game_index: index,
                game_id: game.id.clone(),
                error: "Game ID cannot be empty".to_string(),
            });
        }

        if game.name.is_empty() {
            warnings.push(ValidationWarning::EmptyName {
                game_index: index,
                game_id: game.id.clone(),
            });
        }

        if game.executables.is_empty() {
            errors.push(ValidationError::NoExecutables {
                game_index: index,
                game_id: game.id.clone(),
            });
        }

        // Validate executables
        for (exe_index, executable) in game.executables.iter().enumerate() {
            if executable.name.is_empty() {
                errors.push(ValidationError::InvalidExecutable {
                    game_index: index,
                    game_id: game.id.clone(),
                    executable_index: exe_index,
                    error: "Executable name cannot be empty".to_string(),
                });
            }

            if !["linux", "win32", "darwin"].contains(&executable.os.as_str()) {
                errors.push(ValidationError::InvalidExecutable {
                    game_index: index,
                    game_id: game.id.clone(),
                    executable_index: exe_index,
                    error: format!("Invalid OS: {}", executable.os),
                });
            }
        }
    }

    fn validate_business_rules(
        &self,
        games: &[GameEntry],
        errors: &mut Vec<ValidationError>,
        warnings: &mut Vec<ValidationWarning>,
    ) {
        let mut seen_ids = HashSet::new();
        let mut seen_names = HashSet::new();

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

            // Check for Linux executables (for our use case)
            let has_linux_executable = game.executables.iter().any(|exe| exe.os == "linux");

            if !has_linux_executable {
                warnings.push(ValidationWarning::NoLinuxExecutable {
                    game_index: index,
                    game_id: game.id.clone(),
                });
            }
        }
    }

    fn calculate_stats(&self, games: &[GameEntry]) -> ValidationStats {
        let mut stats = ValidationStats::default();
        let mut unique_executables = HashSet::new();

        stats.total_games = games.len();

        for game in games {
            let mut has_linux = false;
            let mut has_windows = false;
            let mut has_macos = false;
            let mut has_launcher = false;

            for executable in &game.executables {
                unique_executables.insert(&executable.name);

                match executable.os.as_str() {
                    "linux" => has_linux = true,
                    "win32" => has_windows = true,
                    "darwin" => has_macos = true,
                    _ => {}
                }

                if executable.is_launcher {
                    has_launcher = true;
                }
            }

            if has_linux {
                stats.linux_games += 1;
            }
            if has_windows {
                stats.windows_games += 1;
            }
            if has_macos {
                stats.macos_games += 1;
            }
            if has_launcher {
                stats.games_with_launchers += 1;
            }
        }

        stats.unique_executables = unique_executables.len();
        stats
    }

    fn games_equal(&self, old: &GameEntry, new: &GameEntry) -> bool {
        old.id == new.id
            && old.name == new.name
            && old.aliases == new.aliases
            && old.hook == new.hook
            && old.overlay == new.overlay
            && self.executables_equal(&old.executables, &new.executables)
    }

    fn executables_equal(&self, old: &[ExecutableEntry], new: &[ExecutableEntry]) -> bool {
        if old.len() != new.len() {
            return false;
        }

        for (old_exe, new_exe) in old.iter().zip(new.iter()) {
            if old_exe.name != new_exe.name
                || old_exe.os != new_exe.os
                || old_exe.is_launcher != new_exe.is_launcher
                || old_exe.arguments != new_exe.arguments
            {
                return false;
            }
        }

        true
    }

    fn identify_changes(&self, old: &GameEntry, new: &GameEntry) -> Vec<String> {
        let mut changes = Vec::new();

        if old.name != new.name {
            changes.push(format!("Name: '{}' -> '{}'", old.name, new.name));
        }

        if old.aliases != new.aliases {
            changes.push("Aliases changed".to_string());
        }

        if old.hook != new.hook {
            changes.push(format!("Hook: {} -> {}", old.hook, new.hook));
        }

        if old.overlay != new.overlay {
            changes.push(format!("Overlay: {} -> {}", old.overlay, new.overlay));
        }

        if !self.executables_equal(&old.executables, &new.executables) {
            changes.push("Executables changed".to_string());
        }

        changes
    }
}
