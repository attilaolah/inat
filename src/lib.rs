use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, ToStrError, ACCEPT, CONTENT_TYPE},
    Client, Response, StatusCode, Url,
};
use serde::Deserialize;
use thiserror::Error;

pub struct Api {
    client: Client,
    base_url: String,
}

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("bad status: {0}; {1}")]
    BadStatus(StatusCode, String),

    #[error("missing header: {0}")]
    MissingHeader(HeaderName),

    #[error("bad header {0}: {0}")]
    BadHeader(HeaderName, ToStrError),

    #[error("bad content type: {0}")]
    BadContentType(String),

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
        let url: Url = format!("{}/users/{}", self.base_url, user_id).parse()?;
        let res = self.client.get(url).send().await?;

        if !res.status().is_success() {
            return Err(ApiError::bad_status(res).await);
        }
        ensure_json(&res)?;

        let text = res.text().await?;
        Ok(text)
    }
}

impl ApiError {
    async fn bad_status(res: Response) -> Self {
        Self::BadStatus(res.status(), extract_error(res).await)
    }
}

fn ensure_json(res: &Response) -> Result<(), ApiError> {
    let ct = res
        .headers()
        .get(CONTENT_TYPE)
        .ok_or(ApiError::MissingHeader(CONTENT_TYPE))?
        .to_str()
        .map_err(|err| ApiError::BadHeader(CONTENT_TYPE, err))?
        .trim()
        .to_lowercase();
    let parts: Vec<&str> = ct.split(';').map(|part| part.trim()).collect();
    if (parts.len() > 0 && parts[0] != "application/json")
        || (parts.len() > 1 && parts[1] != "charset=utf-8")
    {
        return Err(ApiError::BadContentType(ct.to_string()));
    }

    Ok(())
}

async fn extract_error(res: Response) -> String {
    let data = res.text().await.unwrap_or("".to_string());
    let api_res: ApiResponse = serde_json::from_str(&data).unwrap_or(ApiResponse { error: None });
    api_res.error.unwrap_or_else(|| data.to_string())
}
