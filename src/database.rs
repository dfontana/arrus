mod http_client;

use anyhow::Context;
use anyhow::bail;
use axum::async_trait;
use http_client::HttpClient;
pub use http_client::HttpConfig;
use kitchen_sink::simple_store::{Fetcher, Store};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub http: HttpConfig,
    pub update_interval: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OperatingSystem {
    Linux,
    #[serde(rename = "win32")]
    Windows,
    #[serde(rename = "darwin")]
    MacOS,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GameEntry {
    pub id: String,
    pub name: String,
    pub executables: Vec<ExecutableEntry>,
    // These fields exist too, but we don't use/need them atm
    // aliases: Vec<String>,
    // hook: bool,
    // overlay: bool,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone, Deserialize, Serialize)]
pub struct ExecutableEntry {
    pub name: String,
    pub os: OperatingSystem,
    #[serde(default)]
    pub is_launcher: bool,
    pub arguments: Option<String>,
}

#[derive(Debug, Clone)]
pub enum DatabaseChange {
    Added,
    Touched(Vec<String>), // game ids
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameDatabase {
    pub entries: Vec<GameEntry>,
    etag: String,
}

impl GameDatabase {
    fn from_slice(v: &[u8], etag: String) -> Result<GameDatabase, anyhow::Error> {
        Ok(GameDatabase {
            entries: serde_json::from_slice(v)?,
            etag,
        })
    }
}

impl TryFrom<Vec<u8>> for GameDatabase {
    type Error = anyhow::Error;

    fn try_from(s: Vec<u8>) -> Result<Self, Self::Error> {
        serde_json::from_slice(&s).context("Failed to deserialize")
    }
}

impl<'a> From<&'a GameDatabase> for Vec<u8> {
    fn from(s: &'a GameDatabase) -> Self {
        serde_json::to_vec(s).expect("GameDatabase could not become vec")
    }
}

#[derive(Clone)]
struct GameDBFetcher(Arc<HttpClient>, broadcast::Sender<DatabaseChange>);
#[async_trait]
impl Fetcher<GameDatabase> for GameDBFetcher {
    #[instrument(skip(self, store))]
    async fn fetch(
        &self,
        store: Option<Store<GameDatabase>>,
    ) -> Result<GameDatabase, anyhow::Error> {
        info!("Fetching database");
        let tag = store.as_ref().map(|s| s.read().etag.to_string());
        let response = self.0.download_with_etag(tag.clone()).await?;
        if response.status == reqwest::StatusCode::NOT_MODIFIED {
            info!("Database not modified (304), skipping update");
            if store.is_none() {
                bail!("Update failed, got 304 when no existing store present");
            }
            let v = (*store.unwrap().read()).clone();
            return Ok(v);
        }
        // Game DB is new which means we need to compute updates.
        let new_db = GameDatabase::from_slice(&response.data, response.etag.unwrap_or_default())?;
        let changes = match store.as_ref() {
            Some(db) => {
                let old_db = db.read();
                GameDBFetcher::diff(&old_db, &new_db)
            }
            None => DatabaseChange::Added,
        };
        if let Err(e) = self.1.send(changes) {
            error!("Failed to notify of DB changes: {}", e);
        }
        Ok(new_db)
    }
}

impl GameDBFetcher {
    fn diff(old_db: &GameDatabase, new_db: &GameDatabase) -> DatabaseChange {
        // Create lookup maps by game ID
        let old_games: HashMap<&String, &GameEntry> =
            old_db.entries.iter().map(|e| (&e.id, e)).collect();
        let new_games: HashMap<&String, &GameEntry> =
            new_db.entries.iter().map(|e| (&e.id, e)).collect();
        let mut touched: Vec<String> = Vec::new();

        for id in new_games.keys() {
            if !old_games.contains_key(id) {
                return DatabaseChange::Added;
            }
        }

        // Find removed games
        for id in old_games.keys() {
            if !new_games.contains_key(id) {
                touched.push((*id).clone());
            }
        }

        // Find modified games - check executables that affect matching
        for (id, new_entry) in &new_games {
            if let Some(old_entry) = old_games.get(id) {
                let new_exe_set: HashSet<&ExecutableEntry> =
                    HashSet::from_iter(new_entry.executables.iter());
                let old_exe_set: HashSet<&ExecutableEntry> =
                    HashSet::from_iter(old_entry.executables.iter());
                if new_exe_set != old_exe_set {
                    touched.push((*id).clone());
                }
            }
        }

        if touched.is_empty() {
            DatabaseChange::None
        } else {
            DatabaseChange::Touched(touched)
        }
    }
}

pub async fn store(
    config: DatabaseConfig,
    changes: broadcast::Sender<DatabaseChange>,
) -> Result<Store<GameDatabase>, anyhow::Error> {
    let mut tmp = std::env::temp_dir();
    tmp.push("discoverable.json");
    debug!("Writing game db to {:?}", tmp);
    let f = GameDBFetcher(Arc::new(HttpClient::new(config.http)?), changes);
    let s = Store::new_with_fetcher(tmp, f.clone()).await?;
    s.scheduled_updates(f, config.update_interval);
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_game(id: &str, name: &str, executables: Vec<ExecutableEntry>) -> GameEntry {
        GameEntry {
            id: id.to_string(),
            name: name.to_string(),
            executables,
        }
    }

    fn create_test_executable(name: &str, os: OperatingSystem) -> ExecutableEntry {
        ExecutableEntry {
            name: name.to_string(),
            os,
            is_launcher: false,
            arguments: None,
        }
    }

    #[test]
    fn test_diff_added() {
        let old_db = GameDatabase {
            entries: vec![create_test_game(
                "game1",
                "Game 1",
                vec![create_test_executable(
                    "game1.exe",
                    OperatingSystem::Windows,
                )],
            )],
            etag: "old".to_string(),
        };

        let new_db = GameDatabase {
            entries: vec![
                create_test_game(
                    "game1",
                    "Game 1",
                    vec![create_test_executable(
                        "game1.exe",
                        OperatingSystem::Windows,
                    )],
                ),
                create_test_game(
                    "game2",
                    "Game 2",
                    vec![create_test_executable(
                        "game2.exe",
                        OperatingSystem::Windows,
                    )],
                ),
            ],
            etag: "new".to_string(),
        };

        let result = GameDBFetcher::diff(&old_db, &new_db);
        assert!(matches!(result, DatabaseChange::Added));
    }

    #[test]
    fn test_diff_touched() {
        let old_db = GameDatabase {
            entries: vec![create_test_game(
                "game1",
                "Game 1",
                vec![create_test_executable(
                    "game1.exe",
                    OperatingSystem::Windows,
                )],
            )],
            etag: "old".to_string(),
        };

        let new_db = GameDatabase {
            entries: vec![create_test_game(
                "game1",
                "Game 1",
                vec![
                    create_test_executable("game1.exe", OperatingSystem::Windows),
                    create_test_executable("game1_linux", OperatingSystem::Linux),
                ],
            )],
            etag: "new".to_string(),
        };

        let result = GameDBFetcher::diff(&old_db, &new_db);
        if let DatabaseChange::Touched(touched_ids) = result {
            assert_eq!(touched_ids, vec!["game1"]);
        } else {
            panic!("Expected DatabaseChange::Touched, got {:?}", result);
        }
    }

    #[test]
    fn test_diff_none() {
        let old_db = GameDatabase {
            entries: vec![create_test_game(
                "game1",
                "Game 1",
                vec![create_test_executable(
                    "game1.exe",
                    OperatingSystem::Windows,
                )],
            )],
            etag: "old".to_string(),
        };

        let new_db = GameDatabase {
            entries: vec![create_test_game(
                "game1",
                "Game 1",
                vec![create_test_executable(
                    "game1.exe",
                    OperatingSystem::Windows,
                )],
            )],
            etag: "new".to_string(),
        };

        let result = GameDBFetcher::diff(&old_db, &new_db);
        assert!(matches!(result, DatabaseChange::None));
    }
}
