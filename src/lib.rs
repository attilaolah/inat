use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, ToStrError, ACCEPT, CONTENT_TYPE},
    Client, Response, StatusCode, Url,
};
use serde::Deserialize;
use serde_json::Value;
use serde_yaml::to_string;
use thiserror::Error;

pub struct Api {
    client: Client,
    base_url: String,
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    // OK case:
    page: Option<u64>,
    per_page: Option<u64>,
    total_results: Option<i64>,
    results: Option<Vec<Value>>,

    // Error case:
    status: Option<u16>,
    error: Option<String>,
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

    #[error("response error: {0}")]
    ResponseError(String),

    #[error("response data error: {0}")]
    ResponseDataError(String),

    #[error("failed to decode response data: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("failed to encode response data: {0}")]
    SerdeYamlError(#[from] serde_yaml::Error),

    #[error(transparent)]
    UrlError(#[from] url::ParseError),

    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
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

        let api_res: ApiResponse = serde_json::from_str(&res.text().await?)?;
        ensure_ok(&api_res)?;

        Ok(to_string(&extract_single_value(api_res)?)?)
    }
}

impl ApiResponse {
    fn new() -> Self {
        Self {
            page: None,
            per_page: None,
            total_results: None,
            results: None,
            status: None,
            error: None,
        }
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

fn ensure_ok(res: &ApiResponse) -> Result<(), ApiError> {
    if let Some(status) = res.status {
        if let Ok(sc) = StatusCode::from_u16(status) {
            if !sc.is_success() {
                return Err(ApiError::BadStatus(
                    sc,
                    res.error.clone().unwrap_or("".to_string()),
                ));
            }
        }
    }

    if let Some(error) = &res.error {
        return Err(ApiError::ResponseError(error.to_string()));
    }

    Ok(())
}

fn extract_single_value(res: ApiResponse) -> Result<Value, ApiError> {
    if let Some(page) = res.page {
        if page != 1 {
            return Err(ApiError::ResponseDataError(format!(
                "expected page 1, got: {}",
                page
            )));
        }
    }

    if let Some(per_page) = res.per_page {
        if per_page != 1 {
            return Err(ApiError::ResponseDataError(format!(
                "expected 1 result page, got: {}",
                per_page
            )));
        }
    }

    if let Some(total_results) = res.total_results {
        if total_results != 1 {
            return Err(ApiError::ResponseDataError(format!(
                "expected 1 result in total, got: {}",
                total_results
            )));
        }
    }

    Ok(res
        .results
        .ok_or_else(|| ApiError::ResponseDataError("no results".to_string()))?
        .get(0)
        .ok_or_else(|| ApiError::ResponseDataError("empty results array".to_string()))?
        .clone())
}

async fn extract_error(res: Response) -> String {
    let data = res.text().await.unwrap_or("".to_string());
    let api_res: ApiResponse = serde_json::from_str(&data).unwrap_or(ApiResponse::new());
    api_res.error.unwrap_or_else(|| data.to_string())
}
