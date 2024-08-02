mod error;

pub use error::ApiError;

use std::time::Duration;

use chrono::{DateTime, Utc};
use httpdate::parse_http_date;
use reqwest::{
    header::{HeaderMap, HeaderValue, ACCEPT, AGE, CONTENT_TYPE, DATE, ETAG},
    Client, Response, StatusCode, Url,
};
use serde::Deserialize;

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
    results: Option<Vec<serde_json::Value>>,

    // Error case:
    status: Option<u16>,
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

        let header = extract_header(&res)?;
        let api_res: ApiResponse = serde_json::from_str(&res.text().await?)?;
        ensure_ok(&api_res)?;

        let mut result = "".to_string();

        result.push_str(&serde_yaml::to_string(&header)?);
        result.push_str("---\n");
        result.push_str(&serde_yaml::to_string(&extract_single_value(api_res)?)?);
        Ok(result)
    }
}

fn ensure_json(res: &Response) -> Result<(), ApiError> {
    let ct = res
        .headers()
        .get(CONTENT_TYPE)
        .ok_or(ApiError::MissingHeader(CONTENT_TYPE))?
        .to_str()
        .map_err(|err| ApiError::BadHeaderCoding(CONTENT_TYPE, err))?
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

fn extract_header(res: &Response) -> Result<serde_yaml::Mapping, ApiError> {
    let mut header = serde_yaml::Mapping::new();

    if let Some(date) = res.headers().get(DATE) {
        let mut ts: DateTime<Utc> = parse_http_date(
            date.to_str()
                .map_err(|err| ApiError::BadHeaderCoding(DATE, err))?,
        )
        .map_err(|err| ApiError::BadDateFormat(DATE, err))?
        .into();

        if let Some(age_val) = res.headers().get(AGE) {
            let age: u64 = age_val
                .to_str()
                .map_err(|err| ApiError::BadHeaderCoding(AGE, err))?
                .parse()
                .map_err(|err| ApiError::BadIntFormat(AGE, err))?;
            let duration = Duration::from_secs(age);
            ts -= duration;
        }

        header.insert(
            serde_yaml::Value::String(DATE.to_string()),
            serde_yaml::Value::String(ts.to_rfc3339()),
        );
    }

    if let Some(etag) = res.headers().get(ETAG) {
        header.insert(
            serde_yaml::Value::String(ETAG.to_string()),
            serde_yaml::Value::String(
                etag.to_str()
                    .map_err(|err| ApiError::BadHeaderCoding(ETAG, err))?
                    .to_string(),
            ),
        );
    }

    Ok(header)
}

macro_rules! check_property {
    ($res:expr, $field:ident, $expected:expr) => {
        if let Some(value) = $res.$field {
            if value != $expected {
                return Err(ApiError::ResponseDataError(format!(
                    "expected {}: {}; got: {}",
                    stringify!($field),
                    $expected,
                    value
                )));
            }
        }
    };
}

fn extract_single_value(res: ApiResponse) -> Result<serde_json::Value, ApiError> {
    check_property!(res, page, 1);
    check_property!(res, per_page, 1);
    check_property!(res, total_results, 1);

    Ok(res
        .results
        .ok_or_else(|| ApiError::ResponseDataError("no results".to_string()))?
        .get(0)
        .ok_or_else(|| ApiError::ResponseDataError("empty results array".to_string()))?
        .clone())
}
