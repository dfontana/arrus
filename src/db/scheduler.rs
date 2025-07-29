use crate::db::error::{DatabaseError, Result};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub enable_auto_updates: bool,
    pub update_interval: Duration,
    pub max_update_attempts: u32,
    pub backoff_multiplier: f64,
    pub startup_delay: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            enable_auto_updates: true,
            update_interval: Duration::from_secs(24 * 3600), // 24 hours
            max_update_attempts: 3,
            backoff_multiplier: 2.0,
            startup_delay: Duration::from_secs(5 * 60), // 5 minutes
        }
    }
}

#[derive(Debug, Clone)]
pub enum UpdateTrigger {
    Scheduled,
    Manual,
    Startup,
    ProcessDetectionRequest,
}

#[derive(Debug, Clone)]
pub enum UpdateStatus {
    Started(UpdateTrigger),
    Success(UpdateResult),
    Failed(String),
    Disabled,
}

#[derive(Debug, Clone)]
pub struct UpdateResult {
    pub trigger: UpdateTrigger,
    pub started_at: SystemTime,
    pub completed_at: SystemTime,
    pub success: bool,
    pub games_added: usize,
    pub games_removed: usize,
    pub games_modified: usize,
    pub error: Option<String>,
}

pub struct UpdateScheduler {
    config: SchedulerConfig,
    task_handle: Option<JoinHandle<()>>,
    update_sender: Option<mpsc::UnboundedSender<UpdateTrigger>>,
    status_receiver: Option<mpsc::UnboundedReceiver<UpdateStatus>>,
    last_update_result: Option<UpdateResult>,
    next_scheduled_update: Option<SystemTime>,
}

impl UpdateScheduler {
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            task_handle: None,
            update_sender: None,
            status_receiver: None,
            last_update_result: None,
            next_scheduled_update: None,
        }
    }

    pub async fn start<F, Fut>(&mut self, update_callback: F) -> Result<()>
    where
        F: Fn(UpdateTrigger) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<UpdateResult>> + Send,
    {
        if self.task_handle.is_some() {
            return Err(DatabaseError::SchedulerError(
                "Scheduler already running".to_string(),
            ));
        }

        let (update_tx, mut update_rx) = mpsc::unbounded_channel::<UpdateTrigger>();
        let (status_tx, status_rx) = mpsc::unbounded_channel::<UpdateStatus>();

        self.update_sender = Some(update_tx.clone());
        self.status_receiver = Some(status_rx);

        let config = self.config.clone();
        let callback = Arc::new(update_callback);

        let task_handle = tokio::spawn(async move {
            let mut next_update = if config.enable_auto_updates {
                Some(SystemTime::now() + config.startup_delay)
            } else {
                None
            };

            loop {
                let sleep_duration = if let Some(next) = next_update {
                    let now = SystemTime::now();
                    if now >= next {
                        // Time for scheduled update
                        if let Err(e) = update_tx.send(UpdateTrigger::Scheduled) {
                            tracing::error!("Failed to send scheduled update trigger: {}", e);
                            break;
                        }
                        next_update = Some(now + config.update_interval);
                        config.update_interval
                    } else {
                        next.duration_since(now).unwrap_or(Duration::from_secs(1))
                    }
                } else {
                    // No scheduled updates, just wait for manual triggers
                    Duration::from_secs(60)
                };

                tokio::select! {
                    // Handle update triggers
                    trigger = update_rx.recv() => {
                        if let Some(trigger) = trigger {
                            let _ = status_tx.send(UpdateStatus::Started(trigger.clone()));

                            match callback(trigger.clone()).await {
                                Ok(result) => {
                                    let _ = status_tx.send(UpdateStatus::Success(result));
                                }
                                Err(e) => {
                                    let _ = status_tx.send(UpdateStatus::Failed(e.to_string()));
                                }
                            }
                        } else {
                            // Channel closed
                            break;
                        }
                    }

                    // Sleep until next check
                    _ = tokio::time::sleep(sleep_duration) => {
                        // Continue loop
                    }
                }
            }
        });

        self.task_handle = Some(task_handle);
        self.calculate_next_update();

        tracing::info!("Update scheduler started");
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
            let _ = handle.await;
        }

        self.update_sender = None;
        self.status_receiver = None;

        tracing::info!("Update scheduler stopped");
        Ok(())
    }

    pub async fn trigger_manual_update(&self) -> Result<()> {
        if let Some(sender) = &self.update_sender {
            sender.send(UpdateTrigger::Manual).map_err(|_| {
                DatabaseError::SchedulerError("Failed to send manual update trigger".to_string())
            })?;
            tracing::info!("Manual update triggered");
        } else {
            return Err(DatabaseError::SchedulerError(
                "Scheduler not running".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn trigger_startup_update(&self) -> Result<()> {
        if let Some(sender) = &self.update_sender {
            sender.send(UpdateTrigger::Startup).map_err(|_| {
                DatabaseError::SchedulerError("Failed to send startup update trigger".to_string())
            })?;
            tracing::info!("Startup update triggered");
        }
        Ok(())
    }

    pub async fn trigger_process_detection_update(&self) -> Result<()> {
        if let Some(sender) = &self.update_sender {
            sender
                .send(UpdateTrigger::ProcessDetectionRequest)
                .map_err(|_| {
                    DatabaseError::SchedulerError(
                        "Failed to send process detection update trigger".to_string(),
                    )
                })?;
            tracing::info!("Process detection update triggered");
        }
        Ok(())
    }

    pub fn get_next_scheduled_update(&self) -> Option<SystemTime> {
        self.next_scheduled_update
    }

    pub fn get_last_update_result(&self) -> Option<&UpdateResult> {
        self.last_update_result.as_ref()
    }

    pub async fn poll_status(&mut self) -> Option<UpdateStatus> {
        if let Some(receiver) = &mut self.status_receiver {
            receiver.try_recv().ok()
        } else {
            None
        }
    }

    pub fn is_running(&self) -> bool {
        self.task_handle.is_some()
    }

    pub fn is_auto_updates_enabled(&self) -> bool {
        self.config.enable_auto_updates
    }

    fn calculate_next_update(&mut self) {
        if self.config.enable_auto_updates {
            self.next_scheduled_update = Some(SystemTime::now() + self.config.startup_delay);
        } else {
            self.next_scheduled_update = None;
        }
    }

    pub fn update_config(&mut self, new_config: SchedulerConfig) {
        self.config = new_config;
        self.calculate_next_update();
    }
}

impl Drop for UpdateScheduler {
    fn drop(&mut self) {
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }
}
