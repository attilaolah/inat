use chrono::OutOfRangeError;
use core::num::ParseIntError;
use reqwest::{
    header::{HeaderName, ToStrError},
    Response, StatusCode,
};
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Deserialize)]
struct ApiResponse {
    error: Option<String>,
}

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("bad status: {0}; {1}")]
    BadStatus(StatusCode, String),

    #[error("missing header: {0}")]
    MissingHeader(HeaderName),

    #[error("bad header {0}: {0}")]
    BadHeaderCoding(HeaderName, ToStrError),

    #[error("bad integer header {0}: {0}")]
    BadIntFormat(HeaderName, ParseIntError),

    #[error("bad integer header range {0}: {0}")]
    BadIntRange(HeaderName, OutOfRangeError),

    #[error("bad date header {0}: {0}")]
    BadDateFormat(HeaderName, httpdate::Error),

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

impl ApiError {
    pub async fn bad_status(res: Response) -> Self {
        Self::BadStatus(res.status(), extract_error(res).await)
    }
}

async fn extract_error(res: Response) -> String {
    let data = res.text().await.unwrap_or("".to_string());
    let api_res: ApiResponse = serde_json::from_str(&data).unwrap_or(ApiResponse { error: None });
    api_res.error.unwrap_or_else(|| data.to_string())
}
