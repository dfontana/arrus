use crate::db::error::{DatabaseError, Result};
use reqwest::{Client, StatusCode};
use std::time::Duration;

#[allow(dead_code)]
#[derive(Debug)]
pub struct DatabaseResponse {
    pub data: Vec<u8>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_length: Option<u64>,
    pub status: StatusCode,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub base_url: String,
    pub endpoint: String,
    pub user_agent: String,
    pub timeout: Duration,
    pub connect_timeout: Duration,
    pub max_retries: u32,
    pub retry_delay: Duration,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            base_url: "https://discord.com/api/v9".to_string(),
            endpoint: "/applications/detectable".to_string(),
            user_agent: "arRPC-Rust/1.0".to_string(),
            timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
        }
    }
}

#[allow(dead_code)]
pub struct HttpClient {
    client: Client,
    config: HttpConfig,
}

#[allow(dead_code)]
impl HttpClient {
    pub fn new(config: HttpConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.timeout)
            .connect_timeout(config.connect_timeout)
            .user_agent(&config.user_agent)
            .build()?;

        Ok(Self { client, config })
    }

    pub async fn download_database(&self) -> Result<DatabaseResponse> {
        self.download_with_etag(None).await
    }

    pub async fn download_with_etag(&self, etag: Option<&str>) -> Result<DatabaseResponse> {
        let url = format!("{}{}", self.config.base_url, self.config.endpoint);

        let mut attempt = 0;
        loop {
            attempt += 1;

            let mut request = self.client.get(&url).header("Accept", "application/json");

            if let Some(etag_value) = etag {
                request = request.header("If-None-Match", etag_value);
            }

            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    let headers = response.headers().clone();

                    match status {
                        StatusCode::OK => {
                            let data = response.bytes().await?;

                            return Ok(DatabaseResponse {
                                data: data.to_vec(),
                                etag: headers
                                    .get("etag")
                                    .and_then(|v| v.to_str().ok())
                                    .map(String::from),
                                last_modified: headers
                                    .get("last-modified")
                                    .and_then(|v| v.to_str().ok())
                                    .map(String::from),
                                content_length: headers
                                    .get("content-length")
                                    .and_then(|v| v.to_str().ok())
                                    .and_then(|v| v.parse().ok()),
                                status: StatusCode::OK,
                            });
                        }
                        StatusCode::NOT_MODIFIED => {
                            return Ok(DatabaseResponse {
                                data: Vec::new(),
                                etag: None,
                                last_modified: None,
                                content_length: Some(0),
                                status: StatusCode::NOT_MODIFIED,
                            });
                        }
                        _ => {
                            if attempt >= self.config.max_retries {
                                return Err(DatabaseError::HttpError {
                                    status: status.as_u16(),
                                    message: format!("HTTP {} after {} attempts", status, attempt),
                                });
                            }
                        }
                    }
                }
                Err(e) if attempt >= self.config.max_retries => {
                    return Err(DatabaseError::NetworkError(e));
                }
                Err(_) => {
                    // Retry with exponential backoff
                    let delay = self.config.retry_delay * (2_u32.pow(attempt - 1));
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    pub async fn check_database_version(&self) -> Result<String> {
        let url = format!("{}{}", self.config.base_url, self.config.endpoint);
        let response = self.client.head(&url).send().await?;

        if response.status().is_success() {
            Ok(response
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown")
                .to_string())
        } else {
            Err(DatabaseError::HttpError {
                status: response.status().as_u16(),
                message: "Failed to check database version".to_string(),
            })
        }
    }
}
