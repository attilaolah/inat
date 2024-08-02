use reqwest::{
    header::{HeaderMap, HeaderValue, ACCEPT},
    Client, StatusCode, Url,
};
use serde::Deserialize;
use thiserror::Error;

pub struct Api {
    client: Client,
    base_url: String,
}

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("status {0}: {1}")]
    BadStatus(StatusCode, String),

    #[error(transparent)]
    BadUrl(#[from] url::ParseError),

    #[error(transparent)]
    Failed(#[from] reqwest::Error),
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    error: Option<String>,
}

impl Api {
    pub fn new(base_url: &str) -> Result<Self, ApiError> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        Ok(Self {
            client: Client::builder()
                .default_headers(headers)
                .https_only(true)
                .build()?,
            base_url: base_url.to_string(),
        })
    }
    pub async fn user(&self, user_id: &str) -> Result<String, ApiError> {
        let url = format!("{}/users/{}", self.base_url, user_id);
        let res = self.client.get(url.parse::<Url>()?).send().await?;

        if res.status().is_success() {
            let ct = res.headers().get("content-type");
            match ct {
                Some(val) => match val.to_str() {
                    Ok(val) => println!("ct ok: {}", val),
                    Err(err) => println!("ct err: {}", err),
                },
                None => println!("ct not set"),
            };

            let text = res.text().await?;
            println!("{}", text);
            Ok(text)
        } else {
            Err(ApiError::BadStatus(
                res.status(),
                extract_error(&res.text().await.unwrap_or("".to_string())),
            ))
        }
    }
}

fn extract_error(data: &str) -> String {
    let res: ApiResponse =
        serde_json::from_str(data).unwrap_or_else(|_| ApiResponse { error: None });
    res.error.unwrap_or_else(|| data.to_string())
}
