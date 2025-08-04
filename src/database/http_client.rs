use anyhow::bail;
use reqwest::{Client, StatusCode};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use std::time::Duration;

#[derive(Debug)]
pub struct DatabaseResponse {
    pub data: Vec<u8>,
    pub etag: Option<String>,
    pub status: StatusCode,
}

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub base_url: String,
    pub endpoint: String,
    pub user_agent: String,
    pub timeout: Duration,
    pub connect_timeout: Duration,
    pub max_retries: u32,
}

pub struct HttpClient {
    client: ClientWithMiddleware,
    config: HttpConfig,
}

impl HttpClient {
    pub fn new(config: HttpConfig) -> Result<Self, anyhow::Error> {
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(config.max_retries);
        let client = ClientBuilder::new(
            Client::builder()
                .timeout(config.timeout)
                .connect_timeout(config.connect_timeout)
                .user_agent(&config.user_agent)
                .build()?,
        )
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

        Ok(Self { client, config })
    }

    pub async fn download_with_etag(
        &self,
        etag: Option<String>,
    ) -> Result<DatabaseResponse, anyhow::Error> {
        let url = format!("{}{}", self.config.base_url, self.config.endpoint);

        let mut request = self.client.get(&url).header("Accept", "application/json");
        if let Some(etag_value) = etag {
            request = request.header("If-None-Match", etag_value);
        }
        let response = request.send().await?;
        let status = response.status();
        let headers = response.headers().clone();

        match status {
            StatusCode::OK => {
                let data = response.bytes().await?;
                Ok(DatabaseResponse {
                    data: data.to_vec(),
                    etag: headers
                        .get("etag")
                        .and_then(|v| v.to_str().ok())
                        .map(String::from),
                    status: StatusCode::OK,
                })
            }
            StatusCode::NOT_MODIFIED => Ok(DatabaseResponse {
                data: Vec::new(),
                etag: None,
                status: StatusCode::NOT_MODIFIED,
            }),
            _ => bail!("Http Err: {} {}", status.as_u16(), status),
        }
    }
}
