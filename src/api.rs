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
    Client, RequestBuilder, Response, StatusCode, Url,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use serde_yaml::{
    Deserializer as YamlDeserializer, Mapping as YamlMapping, Sequence as YamlSequence,
    Value as YamlValue,
};

use crate::error::{bad_status, corrupt_cache, internal, Error};

const ID: &str = "id";
//
// Documented as 500, but in practice it seems to be 200.
const MAX_PER_PAGE: &str = "200";

pub struct Api {
    client: Client,
    base_url: Url,
    data_dir: PathBuf,
}

pub struct ApiResults {
    header: YamlMapping,
    body: Vec<JsonValue>,
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    // OK case:
    page: Option<u64>,
    per_page: Option<u64>,
    total_results: Option<u64>,
    results: Option<Vec<JsonValue>>,

    // Error case:
    status: Option<u16>,
    error: Option<String>,
}

#[derive(Debug)]
pub struct Cache {
    header: CacheHeader,
    id: u64,
}

#[derive(Debug)]
pub struct IDsCache {
    header: CacheHeader,
    ids: Vec<u64>,
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
            base_url: base_url.parse()?,
            data_dir: PathBuf::from(data_dir),
        })
    }

    pub async fn sync_all(&self, username: &str) -> Result<(), Error> {
        for subdir in ["observations", "users"] {
            create_dir_all(self.path(subdir))?;
        }

        let user_id = self.sync_user(username).await?;
        println!("OK: users/{}", user_id);

        self.sync_observation_ids(user_id).await?;

        Ok(())
    }

    async fn sync_user(&self, username: &str) -> Result<u64, Error> {
        let cached = self.read_user(username)?;
        let cached_id = cached.as_ref().and_then(|c| Some(c.id));
        let user = match self
            .fetch_user(cached.and_then(|c| Some(c.header)), username)
            .await?
        {
            Some(user) => user,
            // If nothing was returned, it was a cache hit, no need to update.
            _ => return Ok(cached_id.ok_or(internal("user cache missing id"))?),
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

        let cache_path = self.path("users").join(format!("{}.yaml", id));
        write_cache(
            &cache_path,
            &user.header,
            &user.body.first().ok_or(internal("user has no body"))?,
        )?;

        self.symlink_user(&login, &id)?;

        Ok(id)
    }

    async fn sync_observation_ids(&self, user_id: u64) -> Result<Vec<u64>, Error> {
        let mut url = self.endpoint("/observations");
        for (key, val) in [
            // keep sorted
            ("only_id", "true"),
            ("order", "asc"),
            ("order_by", ID),
            ("per_page", MAX_PER_PAGE),
            ("user_id", &user_id.to_string()),
        ] {
            url.query_pairs_mut().append_pair(key, val);
        }

        let cache_path = self
            .path("users")
            .join(format!("{}.observations.yaml", user_id));
        if let Some(cached) = self.read_observation_ids(&cache_path)? {
            println!("cached: {:#?}", cached);
            todo!();
        }

        // No cache is present, simply fetch all IDs.
        let mut ids: Vec<u64> = vec![];
        let last_header: YamlMapping;
        loop {
            let mut url = url.clone();
            if let Some(id) = ids.last() {
                url.query_pairs_mut()
                    .append_pair("id_above", &id.to_string());
            }

            let (mut header, res) = match fetch(self.client.get(url)).await? {
                Some(val) => val,
                // If nothing was returned, it was a cache hit, no need to update.
                _ => continue,
            };

            let is_last = is_last_page(&res)?;
            ids.extend_from_slice(&extract_ids(res)?);

            if is_last {
                // No need to store the etag since it won't be used.
                header.remove(YamlValue::String(ETAG.to_string()));
                last_header = header;
                break;
            }
        }

        write_cache(&cache_path, &last_header, &ids)?;

        Ok(ids)
    }

    async fn fetch_user(
        &self,
        cache: Option<CacheHeader>,
        username: &str,
    ) -> Result<Option<ApiResults>, Error> {
        let url = self.endpoint(&format!("/users/{}", username));
        let mut req = self.client.get(url);
        if let Some(cache) = cache {
            req = req.header(IF_MODIFIED_SINCE, fmt_http_date(cache.date.into()));
            if let Some(etag) = cache.etag {
                req = req.header(IF_NONE_MATCH, etag);
            }
        }

        match fetch(req).await? {
            Some((header, res)) => Ok(Some(ApiResults {
                header,
                body: vec![extract_single_value(res)?],
            })),
            _ => Ok(None),
        }
    }

    fn read_user(&self, username: &str) -> Result<Option<Cache>, Error> {
        lookup_cache_id(&self.path("users").join(format!("{}.yaml", username)))
    }

    fn read_observation_ids(&self, cache_path: &Path) -> Result<Option<IDsCache>, Error> {
        lookup_cache_ids(cache_path)
    }

    fn symlink_user(&self, username: &str, id: &u64) -> Result<(), IoError> {
        // TODO: Remove all obsolete symlinks!
        // TODO: Overwrite existing symlinks (or don't change them)!
        symlink(
            format!("{}.yaml", id),
            self.path("users").join(format!("{}.yaml", username)),
        )?;

        Ok(())
    }

    fn path(&self, sub: &str) -> PathBuf {
        self.data_dir.join(sub)
    }

    fn endpoint(&self, path: &str) -> Url {
        let mut url = self.base_url.clone();
        url.set_path(&format!("{}{}", url.path(), path));
        url
    }
}

async fn fetch(req: RequestBuilder) -> Result<Option<(YamlMapping, ApiResponse)>, Error> {
    let res = req.send().await?;
    if !res.status().is_success() {
        if res.status() == StatusCode::NOT_MODIFIED {
            return Ok(None); // keep using the cache
        }
        return Err(bad_status(res).await);
    }

    ensure_json(&res)?;
    let header = extract_header(&res)?;
    let api_res: ApiResponse = serde_json::from_slice(&res.bytes().await?)?;
    ensure_ok(&api_res)?;

    Ok(Some((header, api_res)))
}

fn ensure_json(res: &Response) -> Result<(), Error> {
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

fn extract_header(res: &Response) -> Result<YamlMapping, Error> {
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

fn lookup_cache_id(path: &Path) -> Result<Option<Cache>, Error> {
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

fn lookup_cache_ids(path: &Path) -> Result<Option<IDsCache>, Error> {
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

fn is_last_page(res: &ApiResponse) -> Result<bool, Error> {
    let page = expect_prop!(res, page);
    let per_page = expect_prop!(res, per_page);
    let total_results = expect_prop!(res, total_results);

    Ok((total_results + per_page - 1u64) / per_page == page)
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

fn extract_single_value(res: ApiResponse) -> Result<JsonValue, Error> {
    check_prop!(res, page, 1);
    check_prop!(res, per_page, 1);
    check_prop!(res, total_results, 1);

    Ok(expect_results(res)?
        .get(0)
        .ok_or(internal("empty results array"))?
        .clone())
}

fn extract_ids(res: ApiResponse) -> Result<Vec<u64>, Error> {
    expect_results(res)?
        .into_iter()
        .map(|val| val.get(ID).cloned())
        .map(|opt| opt.ok_or(internal("missing id")))
        .map(|res| res.and_then(|val| val.as_u64().ok_or(internal("id is not u64"))))
        .collect()
}

fn expect_results(res: ApiResponse) -> Result<Vec<JsonValue>, Error> {
    res.results.ok_or(internal("no results"))
}

fn write_cache<H: Serialize, D: Serialize>(path: &Path, header: &H, data: &D) -> Result<(), Error> {
    let file = File::create(path)?;
    serde_yaml::to_writer(&file, header)?;
    writeln!(&file, "---")?;
    serde_yaml::to_writer(&file, &data)?;

    Ok(())
}
