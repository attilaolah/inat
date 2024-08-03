use std::{
    fs::{create_dir_all, File},
    io::{BufReader, Error as IoError, Write},
    os::unix::fs::symlink,
    path::{Path, PathBuf},
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

use crate::error::{bad_status, corrupt_cache, internal, Error};

const ID: &str = "id";

pub struct Api {
    client: Client,
    base_url: String,
    data_dir: PathBuf,
}

pub struct ApiResults {
    header: serde_yaml::Mapping,
    body: Vec<serde_json::Value>,
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

#[derive(Debug)]
pub struct Cache {
    header: CacheHeader,
    id: u64,
}

#[derive(Debug, Deserialize)]
pub struct CacheHeader {
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
            data_dir: PathBuf::from(data_dir),
        })
    }

    pub async fn sync_all(&self, username: &str) -> Result<(), Error> {
        create_dir_all(self.path("observations"))?;
        create_dir_all(self.path("users"))?;

        let user_id = self.sync_user(username).await?;
        println!("OK: users/{}", user_id);

        Ok(())
    }

    pub async fn sync_user(&self, username: &str) -> Result<u64, Error> {
        let cached = self.read_user(username)?;
        let cached_id = cached.as_ref().and_then(|c| Some(c.id));
        let user = match self
            .fetch_user(cached.and_then(|c| Some(c.header)), username)
            .await?
        {
            // If nothing was returned, it was a cache hit, no need to update.
            None => return Ok(cached_id.ok_or(internal("user cache missing id"))?),
            Some(user) => user,
        };

        let body = user.body.get(0).ok_or(internal("no user returned"))?;
        let id = body
            .get(ID)
            .ok_or(internal("user id not found"))?
            .as_u64()
            .ok_or(internal("user id was not u64"))?;
        let login = body
            .get("login")
            .ok_or(internal("user login not found"))?
            .as_str()
            .ok_or(internal("user login was not a string"))?
            .to_string();

        let filename = self.path("users").join(format!("{}.yaml", id));
        let file = File::create(filename)?;
        serde_yaml::to_writer(&file, &user.header)?;
        writeln!(&file, "---")?;
        for record in user.body {
            serde_yaml::to_writer(&file, &record)?
        }

        self.symlink_user(&login, &id)?;

        Ok(id)
    }

    async fn fetch_user(
        &self,
        cache: Option<CacheHeader>,
        username: &str,
    ) -> Result<Option<ApiResults>, Error> {
        let url: Url = format!("{}/users/{}", self.base_url, username).parse()?;
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
            return Err(bad_status(res).await);
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

    fn read_user(&self, username: &str) -> Result<Option<Cache>, Error> {
        lookup_cache(&self.path("users").join(format!("{}.yaml", username)))
    }

    fn symlink_user(&self, username: &str, id: &u64) -> Result<(), IoError> {
        symlink(
            format!("{}.yaml", id),
            self.path("users").join(format!("{}.yaml", username)),
        )?;

        Ok(())
    }

    fn path(&self, sub: &str) -> PathBuf {
        self.data_dir.join(sub)
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

fn lookup_cache(path: &Path) -> Result<Option<Cache>, Error> {
    match File::open(path) {
        Ok(f) => {
            let mut des = serde_yaml::Deserializer::from_reader(BufReader::new(f));
            if let Some(chunk) = des.next() {
                let header = CacheHeader::deserialize(chunk)?;
                match des.next() {
                    Some(data) => Ok(Some(Cache {
                        header,
                        id: serde_yaml::Mapping::deserialize(data)?
                            .get(ID)
                            .ok_or(corrupt_cache(path, "missing id"))?
                            .as_u64()
                            .ok_or(corrupt_cache(path, "id is not u64"))?,
                    })),
                    None => Err(corrupt_cache(path, "contains only one document")),
                }
            } else {
                Err(corrupt_cache(path, "contains no document"))
            }
        }
        Err(_) => Ok(None),
    }
}

macro_rules! check_property {
    ($res:expr, $field:ident, $expected:expr) => {
        if let Some(value) = $res.$field {
            if value != $expected {
                return Err(internal(&format!(
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
        .ok_or_else(|| internal("no results"))?
        .get(0)
        .ok_or_else(|| internal("empty results array"))?
        .clone())
}
