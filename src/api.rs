use std::{
    fs::{create_dir_all, File},
    io::{BufReader, Write},
    path::{Path, PathBuf},
    thread::sleep,
    time::Duration,
};

use chrono::{DateTime, Utc};
use httpdate::parse_http_date;
use reqwest::{
    header::{HeaderMap, HeaderValue, ACCEPT, AGE, CONTENT_TYPE, DATE, ETAG, RETRY_AFTER},
    Client, RequestBuilder, Response, StatusCode, Url,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use serde_yaml::{
    Deserializer as YamlDeserializer, Mapping as YamlMapping, Sequence as YamlSequence,
    Value as YamlValue,
};
use tracing::debug;

use crate::error::{bad_status, corrupt_cache, internal, Error};

pub(crate) const ID: &str = "id";

// In case no Retry-After header is returned, default to 1m as documented.
// TODO(https://github.com/rust-lang/rust/issues/120301): Use from_mins().
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(60);

pub struct Api {
    pub(crate) client: Client,
    pub(crate) data_dir: PathBuf,
    base_url: Url,
}

pub(crate) struct ApiResults {
    pub(crate) header: YamlMapping,
    pub(crate) body: Vec<JsonMap<String, JsonValue>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ApiResponse {
    // OK case:
    page: Option<u64>,
    per_page: Option<u64>,
    total_results: Option<u64>,
    results: Option<Vec<JsonMap<String, JsonValue>>>,

    // Error case:
    status: Option<u16>,
    error: Option<String>,
}

#[derive(Debug)]
pub(crate) struct Cache {
    pub(crate) header: CacheHeader,
    pub(crate) id: u64,
}

#[derive(Debug)]
pub(crate) struct IDsCache {
    pub(crate) header: CacheHeader,
    pub(crate) ids: Vec<u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CacheHeader {
    pub(crate) date: DateTime<Utc>,
    pub(crate) etag: Option<String>,
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
            base_url: base_url.parse()?,
            data_dir: PathBuf::from(data_dir),
        })
    }

    pub async fn sync_all(&self, username: &str) -> Result<(), Error> {
        for subdir in [
            "applications",
            "comments",
            "conservation_statuses",
            "controlled_term_labels",
            "controlled_terms",
            "faves",
            "flags",
            "identifications",
            "observation_field_values",
            "observation_fields",
            "observation_photos",
            "observations",
            "photos",
            "project_admins",
            "project_observations",
            "projects",
            "quality_metrics",
            "taxa",
            "taxon_changes",
            "users",
            "votes",
        ] {
            create_dir_all(self.path(subdir))?;
        }

        let user_id = self.sync_user(username).await?;
        self.sync_user_observations(user_id).await?;

        Ok(())
    }

    pub(crate) fn path(&self, sub: &str) -> PathBuf {
        self.data_dir.join(sub)
    }

    pub(crate) fn endpoint(&self, path: &str) -> Url {
        let mut url = self.base_url.clone();
        url.set_path(&format!("{}{}", url.path(), path));
        url
    }
}

pub(crate) async fn fetch(
    req: RequestBuilder,
) -> Result<Option<(YamlMapping, ApiResponse)>, Error> {
    let res = loop {
        let res = req
            .try_clone()
            .ok_or(internal("request not cloneable"))?
            .send()
            .await?;
        if res.status().is_success() {
            break res;
        }

        match res.status() {
            StatusCode::NOT_MODIFIED => return Ok(None), // cache hit
            StatusCode::TOO_MANY_REQUESTS => {
                let retry_after = match res.headers().get(RETRY_AFTER) {
                    Some(val) => Duration::from_secs(
                        val.to_str()
                            .map_err(|err| Error::BadHeaderCoding(RETRY_AFTER, err))?
                            .parse()
                            .map_err(|err| Error::BadIntFormat(RETRY_AFTER, err))?,
                    ),
                    _ => DEFAULT_RETRY_AFTER,
                };
                debug!("TOO MANY REQUESTS: sleeping for {}s", retry_after.as_secs());
                sleep(retry_after);
            }
            _ => return Err(bad_status(res).await),
        }
    };

    ensure_json(&res)?;
    let header = extract_header(&res)?;
    let api_res: ApiResponse = serde_json::from_slice(&res.bytes().await?)?;
    ensure_ok(&api_res)?;

    Ok(Some((header, api_res)))
}

pub(crate) fn ensure_json(res: &Response) -> Result<(), Error> {
    let ct = res
        .headers()
        .get(CONTENT_TYPE)
        .ok_or(Error::MissingHeader(CONTENT_TYPE))?
        .to_str()
        .map_err(|err| Error::BadHeaderCoding(CONTENT_TYPE, err))?
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

pub(crate) fn ensure_ok(res: &ApiResponse) -> Result<(), Error> {
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

pub(crate) fn extract_header(res: &Response) -> Result<YamlMapping, Error> {
    let mut header = YamlMapping::new();

    let mut ts: DateTime<Utc> = parse_http_date(
        res.headers()
            .get(DATE)
            .ok_or(Error::MissingHeader(DATE))?
            .to_str()
            .map_err(|err| Error::BadHeaderCoding(DATE, err))?,
    )?
    .into();

    if let Some(age_val) = res.headers().get(AGE) {
        let age: u64 = age_val
            .to_str()
            .map_err(|err| Error::BadHeaderCoding(AGE, err))?
            .parse()
            .map_err(|err| Error::BadIntFormat(AGE, err))?;
        let duration = Duration::from_secs(age);
        ts -= duration;
    }

    header.insert(
        YamlValue::String(DATE.to_string()),
        YamlValue::String(ts.to_rfc3339()),
    );

    if let Some(etag) = res.headers().get(ETAG) {
        header.insert(
            YamlValue::String(ETAG.to_string()),
            YamlValue::String(
                etag.to_str()
                    .map_err(|err| Error::BadHeaderCoding(ETAG, err))?
                    .to_string(),
            ),
        );
    }

    Ok(header)
}

pub(crate) fn lookup_cache_id(path: &Path) -> Result<Option<Cache>, Error> {
    Ok(match lookup_cache(path)? {
        Some((header, data)) => Some(Cache {
            header,
            id: YamlMapping::deserialize(data)?
                .get(ID)
                .ok_or(corrupt_cache(path, "missing id"))?
                .as_u64()
                .ok_or(corrupt_cache(path, "id is not u64"))?,
        }),
        _ => None,
    })
}

pub(crate) fn lookup_cache_ids(path: &Path) -> Result<Option<IDsCache>, Error> {
    Ok(match lookup_cache(path)? {
        Some((header, data)) => Some(IDsCache {
            header,
            ids: YamlSequence::deserialize(data)?
                .into_iter()
                .map(|val| {
                    val.as_u64()
                        .ok_or(corrupt_cache(path, "element is not u64"))
                })
                .collect::<Result<_, _>>()?,
        }),
        _ => None,
    })
}

fn lookup_cache(path: &Path) -> Result<Option<(CacheHeader, YamlDeserializer)>, Error> {
    match File::open(path) {
        Ok(f) => {
            let mut des = serde_yaml::Deserializer::from_reader(BufReader::new(f));
            if let Some(chunk) = des.next() {
                let header = CacheHeader::deserialize(chunk)?;
                match des.next() {
                    Some(data) => Ok(Some((header, data))),
                    _ => Err(corrupt_cache(path, "contains only one document")),
                }
            } else {
                Err(corrupt_cache(path, "contains no document"))
            }
        }
        Err(_) => Ok(None),
    }
}

macro_rules! expect_prop {
    ($res:expr, $field:ident) => {
        $res.$field
            .ok_or(internal(&format!("missing: {}", stringify!($field))))?
    };
}

pub(crate) fn is_last_page(res: &ApiResponse) -> Result<bool, Error> {
    let page = expect_prop!(res, page);
    let per_page = expect_prop!(res, per_page);
    let total_results = expect_prop!(res, total_results);

    Ok((total_results + per_page - 1u64) / per_page <= page)
}

macro_rules! check_prop {
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

pub(crate) fn extract_single_value(res: ApiResponse) -> Result<JsonMap<String, JsonValue>, Error> {
    check_prop!(res, page, 1);
    check_prop!(res, per_page, 1);
    check_prop!(res, total_results, 1);

    Ok(expect_results(res)?
        .get(0)
        .ok_or(internal("empty results array"))?
        .clone())
}

pub(crate) fn extract_ids(res: ApiResponse) -> Result<Vec<u64>, Error> {
    expect_results(res)?.iter().map(extract_id).collect()
}

pub(crate) fn extract_id(obj: &JsonMap<String, JsonValue>) -> Result<u64, Error> {
    obj.get(ID)
        .ok_or(internal("missing id"))
        .and_then(|val| val.as_u64().ok_or(internal("id is not u64")))
}

pub(crate) fn expect_results(res: ApiResponse) -> Result<Vec<JsonMap<String, JsonValue>>, Error> {
    res.results.ok_or(internal("no results"))
}

pub(crate) fn write_cache<H: Serialize, D: Serialize>(
    path: &Path,
    header: &H,
    data: &D,
) -> Result<(), Error> {
    let file = File::create(path)?;
    serde_yaml::to_writer(&file, header)?;
    writeln!(&file, "---")?;
    serde_yaml::to_writer(&file, &data)?;

    Ok(())
}
