use crate::activity::{ActivityData, ActivityMessage, ActivityTimestamps};
use crate::process::database::GameEntry;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

#[derive(Debug)]
struct ActiveGame {
    game_id: String,
    game_name: String,
    pid: u32,
    start_timestamp: u64,
}

pub struct ActivityManager {
    active_games: HashMap<String, ActiveGame>,
    message_sender: mpsc::UnboundedSender<ActivityMessage>,
}

impl ActivityManager {
    pub fn new(message_sender: mpsc::UnboundedSender<ActivityMessage>) -> Self {
        Self {
            active_games: HashMap::new(),
            message_sender,
        }
    }

    pub fn update_detected_games(&mut self, detected: Vec<(&GameEntry, u32)>) {
        let mut current_ids = std::collections::HashSet::new();

        // Process newly detected games
        for (game, pid) in detected {
            current_ids.insert(game.id.clone());

            if !self.active_games.contains_key(&game.id) {
                self.handle_new_game(game, pid);
            } else {
                // Game still active, resend activity (intentional behavior from Node.js version)
                self.send_activity_for_game(&game.id, Some(pid));
            }
        }

        // Remove games that are no longer detected
        let lost_games: Vec<String> = self
            .active_games
            .keys()
            .filter(|id| !current_ids.contains(*id))
            .cloned()
            .collect();

        for game_id in lost_games {
            self.handle_lost_game(&game_id);
        }
    }

    fn handle_new_game(&mut self, game: &GameEntry, pid: u32) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let active_game = ActiveGame {
            game_id: game.id.clone(),
            game_name: game.name.clone(),
            pid,
            start_timestamp: now,
        };

        tracing::info!("Game detected: {} (PID: {})", game.name, pid);

        self.active_games.insert(game.id.clone(), active_game);
        self.send_activity_for_game(&game.id, Some(pid));
    }

    fn handle_lost_game(&mut self, game_id: &str) {
        if let Some(active_game) = self.active_games.remove(game_id) {
            tracing::info!("Game lost: {}", active_game.game_name);

            // Send clear activity message
            let message = ActivityMessage {
                socket_id: game_id.to_string(),
                activity: None,
                pid: Some(active_game.pid),
            };

            if let Err(e) = self.message_sender.send(message) {
                tracing::error!("Failed to send clear activity message: {}", e);
            }
        }
    }

    fn send_activity_for_game(&self, game_id: &str, pid: Option<u32>) {
        if let Some(active_game) = self.active_games.get(game_id) {
            let activity = ActivityData {
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
            };

            let message = ActivityMessage {
                socket_id: game_id.to_string(),
                activity: Some(activity),
                pid,
            };

            if let Err(e) = self.message_sender.send(message) {
                tracing::error!("Failed to send activity message: {}", e);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.active_games.len()
    }
}
