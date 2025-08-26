use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct ActiveGame {
    pub game_id: String,
    pub game_name: String,
    pub pid: u32,
    pub start_timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityMessage {
    #[serde(rename = "socketId")]
    pub socket_id: String,
    activity: Option<ActivityData>,
    pid: Option<u32>,
}

impl ActivityMessage {
    pub fn active(value: &ActiveGame, pid: Option<u32>) -> Self {
        Self {
            socket_id: value.game_id.to_string(),
            activity: Some(value.into()),
            pid,
        }
    }
    pub fn has_activity(&self) -> bool {
        self.activity.is_some()
    }
    pub fn clear(active_game: ActiveGame) -> Self {
        Self {
            socket_id: active_game.game_id.to_string(),
            activity: None,
            pid: Some(active_game.pid),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivityData {
    application_id: String,
    name: String,
    details: Option<String>,
    state: Option<String>,
    timestamps: Option<ActivityTimestamps>,
    assets: Option<ActivityAssets>,
    party: Option<ActivityParty>,
    secrets: Option<ActivitySecrets>,
    instance: Option<bool>,
    flags: Option<u32>,
    buttons: Option<Vec<String>>,
    metadata: Option<ActivityMetadata>,
    #[serde(rename = "type")]
    activity_type: u8,
}

impl From<&ActiveGame> for ActivityData {
    fn from(active_game: &ActiveGame) -> Self {
        Self {
            application_id: active_game.game_id.clone(),
            name: active_game.game_name.clone(),
            details: None,
            state: None,
            timestamps: Some(ActivityTimestamps {
                start: Some(active_game.start_timestamp),
                end: None,
            }),
            assets: None,
            party: None,
            secrets: None,
            instance: None,
            flags: None,
            buttons: None,
            metadata: None,
            activity_type: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivityTimestamps {
    start: Option<u64>,
    end: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivityAssets {
    large_image: Option<String>,
    large_text: Option<String>,
    small_image: Option<String>,
    small_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivityParty {
    id: Option<String>,
    size: Option<[u32; 2]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivitySecrets {
    join: Option<String>,
    spectate: Option<String>,
    #[serde(rename = "match")]
    match_secret: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivityMetadata {
    button_urls: Option<Vec<String>>,
}
