use crate::db::error::{DatabaseError, Result};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::fs as async_fs;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BackupInfo {
    pub id: String,
    pub timestamp: SystemTime,
    pub size: u64,
    pub checksum: String,
    pub version_info: Option<String>,
}

#[allow(dead_code)]
pub struct FileManager {
    database_path: PathBuf,
    temp_dir: PathBuf,
    backup_dir: PathBuf,
}

#[allow(dead_code)]
impl FileManager {
    pub fn new(base_path: PathBuf) -> Result<Self> {
        let temp_dir = base_path.join("temp");
        let backup_dir = base_path.join("backups");
        let database_path = base_path.join("detectable.json");

        // Create directories if they don't exist
        fs::create_dir_all(&temp_dir)?;
        fs::create_dir_all(&backup_dir)?;

        // Create parent directory for database if it doesn't exist
        if let Some(parent) = database_path.parent() {
            fs::create_dir_all(parent)?;
        }

        Ok(Self {
            database_path,
            temp_dir,
            backup_dir,
        })
    }

    pub async fn write_database_atomic(&self, data: &[u8]) -> Result<()> {
        // Create temporary file with unique name
        let temp_id = Uuid::new_v4();
        let temp_path = self
            .temp_dir
            .join(format!("detectable_{}.json.tmp", temp_id));

        // Write to temporary file
        let mut temp_file = async_fs::File::create(&temp_path).await?;
        temp_file.write_all(data).await?;
        temp_file.flush().await?;
        temp_file.sync_all().await?;
        drop(temp_file);

        // Verify written data
        let written_data = async_fs::read(&temp_path).await?;
        if written_data != data {
            let _ = async_fs::remove_file(&temp_path).await;
            return Err(DatabaseError::VerificationFailed);
        }

        // Create backup of current database
        if self.database_path.exists() {
            self.create_backup("pre-update").await?;
        }

        // Atomic move (rename) to replace current database
        async_fs::rename(&temp_path, &self.database_path).await?;

        // Verify final file
        let final_data = async_fs::read(&self.database_path).await?;
        if final_data != data {
            return Err(DatabaseError::AtomicUpdateFailed);
        }

        tracing::info!("Database updated successfully: {} bytes", data.len());
        Ok(())
    }

    pub async fn read_current_database(&self) -> Result<Vec<u8>> {
        async_fs::read(&self.database_path)
            .await
            .map_err(DatabaseError::from)
    }

    pub async fn create_backup(&self, label: &str) -> Result<BackupInfo> {
        if !self.database_path.exists() {
            return Err(DatabaseError::BackupError(
                "No database file to backup".to_string(),
            ));
        }

        let data = self.read_current_database().await?;
        let checksum = format!("{:x}", Sha256::digest(&data));
        let timestamp = SystemTime::now();
        let backup_id = format!(
            "{}_{}",
            timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            label
        );

        let backup_path = self.backup_dir.join(format!("{}.json", backup_id));
        async_fs::write(&backup_path, &data).await?;

        let backup_info = BackupInfo {
            id: backup_id,
            timestamp,
            size: data.len() as u64,
            checksum,
            version_info: None,
        };

        tracing::info!("Created backup: {}", backup_info.id);
        Ok(backup_info)
    }

    pub async fn restore_from_backup(&self, backup_id: &str) -> Result<()> {
        let backup_path = self.backup_dir.join(format!("{}.json", backup_id));

        if !backup_path.exists() {
            return Err(DatabaseError::BackupError(format!(
                "Backup {} not found",
                backup_id
            )));
        }

        let backup_data = async_fs::read(&backup_path).await?;
        self.write_database_atomic(&backup_data).await?;

        tracing::info!("Restored database from backup: {}", backup_id);
        Ok(())
    }

    pub async fn cleanup_temp_files(&self) -> Result<()> {
        let mut dir = async_fs::read_dir(&self.temp_dir).await?;
        let now = SystemTime::now();

        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();

            if let Some(extension) = path.extension() {
                if extension == "tmp" {
                    if let Ok(metadata) = entry.metadata().await {
                        if let Ok(modified) = metadata.modified() {
                            // Delete temp files older than 1 hour
                            if now.duration_since(modified).unwrap_or_default().as_secs() > 3600 {
                                let _ = async_fs::remove_file(&path).await;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn list_backups(&self) -> Result<Vec<BackupInfo>> {
        let mut backups = Vec::new();

        if !self.backup_dir.exists() {
            return Ok(backups);
        }

        for entry in fs::read_dir(&self.backup_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let metadata = entry.metadata()?;
                let size = metadata.len();
                let timestamp = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);

                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    // Read file to calculate checksum
                    let data = fs::read(&path)?;
                    let checksum = format!("{:x}", Sha256::digest(&data));

                    backups.push(BackupInfo {
                        id: filename.to_string(),
                        timestamp,
                        size,
                        checksum,
                        version_info: None,
                    });
                }
            }
        }

        // Sort by timestamp (newest first)
        backups.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(backups)
    }

    pub async fn cleanup_old_backups(&self, keep_count: usize) -> Result<()> {
        let backups = self.list_backups()?;

        if backups.len() > keep_count {
            for backup in backups.iter().skip(keep_count) {
                let backup_path = self.backup_dir.join(format!("{}.json", backup.id));
                let _ = async_fs::remove_file(&backup_path).await;
                tracing::info!("Removed old backup: {}", backup.id);
            }
        }

        Ok(())
    }

    pub fn database_exists(&self) -> bool {
        self.database_path.exists()
    }

    pub fn get_database_path(&self) -> &Path {
        &self.database_path
    }
}
