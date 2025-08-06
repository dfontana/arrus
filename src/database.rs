mod http_client;

use anyhow::Context;
use anyhow::bail;
use axum::async_trait;
use http_client::HttpClient;
pub use http_client::HttpConfig;
use kitchen_sink::simple_store::{Fetcher, Store};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;
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
    pub os: OperatingSystem,
    #[serde(default)]
    pub is_launcher: bool,
    pub arguments: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
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
struct GameDBFetcher(Arc<HttpClient>);
#[async_trait]
impl Fetcher<GameDatabase> for GameDBFetcher {
    #[instrument(skip(self, store))]
    async fn fetch(
        &self,
        store: Option<Store<GameDatabase>>,
    ) -> Result<GameDatabase, anyhow::Error> {
        info!("Fetching database");
        let tag = store.as_ref().map(|s| s.read().etag.to_string());
        let response = self.0.download_with_etag(tag).await?;
        if response.status == reqwest::StatusCode::NOT_MODIFIED {
            info!("Database not modified (304), skipping update");
            if store.is_none() {
                bail!("Update failed, got 304 when no existing store present");
            }
            let v = (*store.unwrap().read()).clone();
            return Ok(v);
        }
        Ok(GameDatabase::from_slice(
            &response.data,
            response.etag.unwrap_or_default(),
        )?)
    }
}

pub async fn store(config: DatabaseConfig) -> Result<Store<GameDatabase>, anyhow::Error> {
    let mut tmp = std::env::temp_dir();
    tmp.push("discoverable.json");
    debug!("Writing game db to {:?}", tmp);
    let f = GameDBFetcher(Arc::new(HttpClient::new(config.http)?));
    let s = Store::new_with_fetcher(tmp, f.clone()).await?;
    s.scheduled_updates(f, config.update_interval);
    Ok(s)
}
