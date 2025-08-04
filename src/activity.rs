mod types;

use crate::database::GameEntry;
use anyhow::{Context, anyhow};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument};
pub use types::*;

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

    #[instrument(skip(self))]
    pub fn update_detected_games(&mut self, detected: Vec<(&GameEntry, u32)>) {
        let mut current_ids = std::collections::HashSet::new();

        // Process newly detected games
        for (game, pid) in detected {
            current_ids.insert(game.id.clone());

            if !self.active_games.contains_key(&game.id) {
                self.handle_new_game(game, pid);
            } else {
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

        self.active_games.insert(game.id.clone(), active_game);
        self.send_activity_for_game(&game.id, Some(pid));
    }

    fn handle_lost_game(&mut self, game_id: &str) {
        info!("Game lost: {}", game_id);
        if let Err(e) = self
            .active_games
            .remove(game_id)
            .map(ActivityMessage::clear)
            .ok_or(anyhow!("Game id was not stored: {}", game_id))
            .and_then(|msg| {
                self.message_sender
                    .send(msg)
                    .context("Failed to send clear activity msg")
            })
        {
            error!("{}", e);
        }
    }

    #[instrument(skip(self))]
    fn send_activity_for_game(&self, game_id: &str, pid: Option<u32>) {
        if let Some(active_game) = self.active_games.get(game_id) {
            debug!("Sending activity");
            if let Err(e) = self
                .message_sender
                .send(ActivityMessage::active(active_game, pid))
            {
                error!("Failed to send activity message: {}", e);
            }
        }
    }
}
