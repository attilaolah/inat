mod error;

use std::{
    fs::{create_dir_all, File},
    io::{BufReader, Write},
    os::unix::fs::symlink,
    time::Duration,
};

use chrono::{DateTime, Utc};
use httpdate::{fmt_http_date, parse_http_date};
use reqwest::{
    header::{
        HeaderMap, HeaderValue, ACCEPT, AGE, CONTENT_TYPE, DATE, ETAG, IF_MODIFIED_SINCE,
        IF_NONE_MATCH,
    },
    Client, Response, StatusCode, Url,
};
use serde::Deserialize;

pub use error::Error;

pub struct Api {
    client: Client,
    base_url: String,
    data_dir: String,
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

pub struct ApiResults {
    header: serde_yaml::Mapping,
    body: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct ApiCache {
    date: DateTime<Utc>,
    etag: Option<String>,
}

impl Api {
    pub fn new(base_url: &str, data_dir: &str) -> Result<Self, Error> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        Ok(Self {
            client: Client::builder()
                .default_headers(headers)
                .https_only(true)
                .build()?,
            base_url: base_url.to_string(),
            data_dir: data_dir.to_string(),
        })
    }

    pub async fn fetch_user(
        &self,
        cache: Option<ApiCache>,
        user_id: &str,
    ) -> Result<Option<ApiResults>, Error> {
        let url: Url = format!("{}/users/{}", self.base_url, user_id).parse()?;
        let mut req = self.client.get(url);
        if let Some(cache) = cache {
            req = req.header(IF_MODIFIED_SINCE, fmt_http_date(cache.date.into()));
            if let Some(etag) = cache.etag {
                req = req.header(IF_NONE_MATCH, etag);
            }
        }

        let res = req.send().await?;
        if !res.status().is_success() {
            if res.status() == StatusCode::NOT_MODIFIED {
                return Ok(None); // keep using the cache
            }
            return Err(error::bad_status(res).await);
        }

        ensure_json(&res)?;
        let header = extract_header(&res)?;
        let api_res: ApiResponse = serde_json::from_str(&res.text().await?)?;
        ensure_ok(&api_res)?;

        Ok(Some(ApiResults {
            header,
            body: vec![extract_single_value(api_res)?],
        }))
    }

    pub fn read_user(&self, user_id: &str) -> Result<Option<ApiCache>, Error> {
        lookup_cache(&format!("{}/users/{}.yaml", self.data_dir, user_id))
    }

    pub async fn sync_user(&self, user_id: &str) -> Result<(), Error> {
        let user = match self.fetch_user(self.read_user(user_id)?, user_id).await? {
            None => return Ok(()), // keep using the cache
            Some(user) => user,
        };

        let body = user
            .body
            .get(0)
            .ok_or(error::internal("no user returned"))?;
        let id = body
            .get("id")
            .ok_or(error::internal("user id not found"))?
            .as_u64()
            .ok_or(error::internal("user id was not an unsigned integer"))?;
        let login = body
            .get("login")
            .ok_or(error::internal("user login not found"))?
            .as_str()
            .ok_or(error::internal("user login was not a string"))?
            .to_string();

        create_dir_all(format!("{}/users", self.data_dir))?;

        let filename = format!("{}/users/{}.yaml", self.data_dir, id);
        let file = File::create(&filename)?;
        serde_yaml::to_writer(&file, &user.header)?;
        writeln!(&file, "---")?;
        for record in user.body {
            serde_yaml::to_writer(&file, &record)?
        }

        symlink(
            format!("{}.yaml", id),
            format!("{}/users/{}.yaml", self.data_dir, login),
        )?;

        Ok(())
    }
}

fn ensure_json(res: &Response) -> Result<(), Error> {
    let ct = res
        .headers()
        .get(CONTENT_TYPE)
        .ok_or(Error::MissingHeader(CONTENT_TYPE))?
        .to_str()
        .map_err(|e| Error::BadHeaderCoding(CONTENT_TYPE, e))?
        .trim()
        .to_lowercase();
    let parts: Vec<&str> = ct.split(';').map(|part| part.trim()).collect();
    if (parts.len() > 0 && parts[0] != "application/json")
        || (parts.len() > 1 && parts[1] != "charset=utf-8")
    {
        return Err(Error::BadContentType(ct.to_string()));
    }

    Ok(())
}

fn ensure_ok(res: &ApiResponse) -> Result<(), Error> {
    if let Some(status) = res.status {
        if let Ok(sc) = StatusCode::from_u16(status) {
            if !sc.is_success() {
                return Err(Error::BadStatus(
                    sc,
                    res.error.clone().unwrap_or("".to_string()),
                ));
            }
        }
    }

    if let Some(error) = &res.error {
        return Err(Error::ResponseError(error.to_string()));
    }

    Ok(())
}

fn extract_header(res: &Response) -> Result<serde_yaml::Mapping, Error> {
    let mut header = serde_yaml::Mapping::new();

    let mut ts: DateTime<Utc> = parse_http_date(
        res.headers()
            .get(DATE)
            .ok_or(Error::MissingHeader(DATE))?
            .to_str()
            .map_err(|e| Error::BadHeaderCoding(DATE, e))?,
    )?
    .into();

    if let Some(age_val) = res.headers().get(AGE) {
        let age: u64 = age_val
            .to_str()
            .map_err(|e| Error::BadHeaderCoding(AGE, e))?
            .parse()
            .map_err(|e| Error::BadIntFormat(AGE, e))?;
        let duration = Duration::from_secs(age);
        ts -= duration;
    }

    header.insert(
        serde_yaml::Value::String(DATE.to_string()),
        serde_yaml::Value::String(ts.to_rfc3339()),
    );

    if let Some(etag) = res.headers().get(ETAG) {
        header.insert(
            serde_yaml::Value::String(ETAG.to_string()),
            serde_yaml::Value::String(
                etag.to_str()
                    .map_err(|e| Error::BadHeaderCoding(ETAG, e))?
                    .to_string(),
            ),
        );
    }

    Ok(header)
}

fn lookup_cache(path: &str) -> Result<Option<ApiCache>, Error> {
    match File::open(path) {
        Ok(f) => {
            if let Some(header) = serde_yaml::Deserializer::from_reader(BufReader::new(f)).next() {
                Ok(Some(ApiCache::deserialize(header)?))
            } else {
                Err(error::internal(&format!("no document found in {}", path)))
            }
        }
        Err(_) => Ok(None),
    }
}

macro_rules! check_property {
    ($res:expr, $field:ident, $expected:expr) => {
        if let Some(value) = $res.$field {
            if value != $expected {
                return Err(error::internal(&format!(
                    "expected {}: {}; got: {}",
                    stringify!($field),
                    $expected,
                    value
                )));
            }
        }
    };
}

fn extract_single_value(res: ApiResponse) -> Result<serde_json::Value, Error> {
    check_property!(res, page, 1);
    check_property!(res, per_page, 1);
    check_property!(res, total_results, 1);

    Ok(res
        .results
        .ok_or_else(|| error::internal("no results"))?
        .get(0)
        .ok_or_else(|| error::internal("empty results array"))?
        .clone())
}
