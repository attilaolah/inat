use std::path::{Path, PathBuf};

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
    error: String,
}

#[derive(Error, Debug)]
pub enum Error {
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

    #[error("bad content type: {0}")]
    BadContentType(String),

    #[error("response error: {0}")]
    ResponseError(String),

    #[error("path {0}: {1}")]
    CorruptCache(PathBuf, String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    HttpDateError(#[from] httpdate::Error),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    SerdeYamlError(#[from] serde_yaml::Error),

    #[error(transparent)]
    UrlError(#[from] url::ParseError),

    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

pub fn internal(msg: &str) -> Error {
    Error::Internal(msg.to_string())
}

pub fn corrupt_cache(path: &Path, msg: &str) -> Error {
    Error::CorruptCache(path.to_path_buf(), msg.to_string())
}

pub async fn bad_status(res: Response) -> Error {
    Error::BadStatus(res.status(), extract_error(res).await)
}

async fn extract_error(res: Response) -> String {
    let data = res.text().await.unwrap_or("".to_string());
    let api_res: Result<ApiResponse, _> = serde_json::from_str(&data);
    match api_res {
        Ok(res) => res.error,
        Err(_) => data.to_string(),
    }
}
